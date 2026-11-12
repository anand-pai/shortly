package loadtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
)

type phase int32

const (
	phaseWarmup phase = iota
	phaseMeasure
)

type kind uint8

const (
	kindWrite kind = iota
	kindRead
)

func kindFor(seq uint64, writePercent int) kind {
	if int(seq%100) < writePercent {
		return kindWrite
	}
	return kindRead
}

type job struct {
	kind  kind
	phase phase
}

// Creator issues one short-URL create. The slug field is left empty.
type Creator interface {
	Create(ctx context.Context, longURL, companyID string) (string, error)
}

// Clicker requests one slug and returns the HTTP status without following redirects.
type Clicker interface {
	Click(ctx context.Context, slug string) (statusCode int, err error)
}

// Runner paces creates and clicks and collects the measured window.
// A Runner is used for one Run.
type Runner struct {
	cfg       Config
	ring      *Ring
	companies []string
	locals    []localStats
	create    Creator
	click     Clicker
	log       *log.Logger

	started time.Time
	window  time.Duration
	warmup  time.Duration

	phase            atomic.Int32
	inflight         atomic.Int64
	scheduledWrites  atomic.Uint64
	scheduledReads   atomic.Uint64
	notStarted       atomic.Uint64
	warmupNotStarted atomic.Uint64
}

// New validates cfg and builds the slug ring and company pool.
func New(cfg Config, create Creator, click Clicker) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if create == nil || click == nil {
		return nil, errors.New("create and click clients are required")
	}
	return &Runner{
		cfg:       cfg,
		ring:      newRing(cfg.RingSize),
		companies: companyIDs(cfg.CompanyPool),
		locals:    make([]localStats, cfg.Workers),
		create:    create,
		click:     click,
		log:       log.New(os.Stderr, "", 0),
	}, nil
}

type point struct {
	at               time.Time
	phase            phase
	filled           int
	inflight         int64
	notStarted       uint64
	warmupNotStarted uint64
	warmup           opSnap
	write            opSnap
	read             opSnap
}

// Run drives warmup and then the measured window. The summary is returned
// even when err is set, so a timed-out warmup still reports what happened.
func (r *Runner) Run(ctx context.Context) (Summary, error) {
	if r.log == nil {
		r.log = log.New(io.Discard, "", 0)
	}
	r.started = time.Now()

	jobs := make(chan job, queueSize(r.cfg))
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.worker(id, jobs)
		}(i)
	}

	stopReport := make(chan struct{})
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		r.report(stopReport)
	}()

	runErr := r.pace(ctx, jobs)
	close(jobs)
	wg.Wait()
	close(stopReport)
	<-reportDone
	return r.summary(), runErr
}

func queueSize(cfg Config) int {
	burst := cfg.RPS / 20
	if burst < 1 {
		burst = 1
	}
	q := cfg.Workers * 2
	if q < burst {
		q = burst
	}
	return q
}

func (r *Runner) worker(id int, jobs <-chan job) {
	rng := rand.New(rand.NewSource(r.cfg.Seed + int64(id)*1_000_003))
	urls := newURLGen(r.cfg.LongURLBytes)
	var zipf *rand.Zipf
	if r.cfg.ReadDistribution == "zipf" && r.ring.Cap() > 1 {
		zipf = rand.NewZipf(rng, 1.2, 1, uint64(r.ring.Cap()-1))
	}
	for job := range jobs {
		r.inflight.Add(1)
		switch job.kind {
		case kindRead:
			r.doRead(id, job.phase, rng, zipf)
		default:
			r.doWrite(id, job.phase, rng, urls)
		}
		r.inflight.Add(-1)
	}
}

func (r *Runner) doWrite(id int, ph phase, rng *rand.Rand, urls *urlGen) {
	company := r.companies[rng.Intn(len(r.companies))]
	longURL := urls.next(rng)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	start := time.Now()
	slug, err := r.create.Create(ctx, longURL, company)
	elapsed := time.Since(start)
	cancel()
	if err != nil {
		key := "write:" + status.Code(err).String()
		if errors.Is(err, errBadSlug) {
			key = "write:BadSlug"
		}
		r.locals[id].record(ph, kindWrite, elapsed, key, false, true)
		return
	}
	r.ring.Push(slug)
	r.locals[id].record(ph, kindWrite, elapsed, "write:OK", true, true)
}

func (r *Runner) doRead(id int, ph phase, rng *rand.Rand, zipf *rand.Zipf) {
	slug, ok := r.ring.Sample(rng, zipf)
	if !ok || !slugPattern.MatchString(slug) {
		key := "read:empty_pool"
		if ok {
			key = "read:BadSlug"
		}
		r.locals[id].record(ph, kindRead, 0, key, false, false)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	start := time.Now()
	code, err := r.click.Click(ctx, slug)
	elapsed := time.Since(start)
	cancel()
	if err != nil {
		r.locals[id].record(ph, kindRead, elapsed, "read:"+netClass(err), false, true)
		return
	}
	r.locals[id].record(ph, kindRead, elapsed, "read:"+strconv.Itoa(code), code == http.StatusFound, true)
}

func (r *Runner) pace(ctx context.Context, jobs chan<- job) error {
	var measureStart time.Time
	if r.cfg.WarmupSlugs == 0 {
		measureStart = r.started
		r.phase.Store(int32(phaseMeasure))
	} else {
		r.phase.Store(int32(phaseWarmup))
	}
	defer func() {
		end := time.Now()
		if !measureStart.IsZero() {
			r.window = time.Since(measureStart)
			end = measureStart
		}
		if r.cfg.WarmupSlugs > 0 {
			r.warmup = end.Sub(r.started)
		}
	}()

	warmupDeadline := r.started.Add(r.cfg.EffectiveWarmupTimeout())
	maxBurst := r.cfg.RPS / 20
	if maxBurst < 1 {
		maxBurst = 1
	}
	ratePerNs := float64(r.cfg.RPS) / float64(time.Second)

	timer := time.NewTimer(0)
	<-timer.C
	defer timer.Stop()

	var (
		acc    float64
		seq    uint64
		last   = time.Now()
		runErr error
	)

loop:
	for {
		if err := ctx.Err(); err != nil {
			runErr = err
			break
		}
		if measureStart.IsZero() && r.ring.Len() >= r.cfg.WarmupSlugs {
			measureStart = time.Now()
			last = measureStart
			acc = 0
			r.phase.Store(int32(phaseMeasure))
			r.log.Printf("warmup complete slugs=%d warmup=%s", r.ring.Len(), measureStart.Sub(r.started).Round(time.Millisecond))
		}
		if measureStart.IsZero() && time.Now().After(warmupDeadline) {
			runErr = fmt.Errorf("warmup timed out with %d/%d slugs stored", r.ring.Len(), r.cfg.WarmupSlugs)
			break
		}
		if !measureStart.IsZero() && time.Since(measureStart) >= r.cfg.Duration {
			break
		}

		phaseNow := phaseWarmup
		if !measureStart.IsZero() {
			phaseNow = phaseMeasure
		}

		now := time.Now()
		acc += float64(now.Sub(last)) * ratePerNs
		last = now
		if int(acc) > maxBurst {
			dropped := int(acc) - maxBurst
			for i := 0; i < dropped; i++ {
				r.dropToken(phaseNow, &seq)
			}
			acc -= float64(dropped)
		}
		n := int(acc)
		acc -= float64(n)
		if n == 0 {
			if err := wait(ctx, timer, time.Millisecond); err != nil {
				runErr = err
				break
			}
			continue
		}

		for i := 0; i < n; i++ {
			if err := ctx.Err(); err != nil {
				runErr = err
				break loop
			}
			op := kindWrite
			if phaseNow == phaseMeasure {
				op = kindFor(seq, r.cfg.WritePercent)
				seq++
				if op == kindWrite {
					r.scheduledWrites.Add(1)
				} else {
					r.scheduledReads.Add(1)
				}
			}
			select {
			case jobs <- job{kind: op, phase: phaseNow}:
			default:
				if phaseNow == phaseMeasure {
					r.notStarted.Add(1)
				} else {
					r.warmupNotStarted.Add(1)
				}
			}
		}
	}
	return runErr
}

func (r *Runner) dropToken(ph phase, seq *uint64) {
	if ph != phaseMeasure {
		r.warmupNotStarted.Add(1)
		return
	}
	op := kindFor(*seq, r.cfg.WritePercent)
	*seq++
	if op == kindWrite {
		r.scheduledWrites.Add(1)
	} else {
		r.scheduledReads.Add(1)
	}
	r.notStarted.Add(1)
}

func wait(ctx context.Context, timer *time.Timer, d time.Duration) error {
	timer.Reset(d)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runner) report(stop <-chan struct{}) {
	r.log.Printf("progress every %s", r.cfg.ReportInterval)
	ticker := time.NewTicker(r.cfg.ReportInterval)
	defer ticker.Stop()
	prev := r.snapshot()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			cur := r.snapshot()
			r.printDelta(prev, cur)
			prev = cur
		}
	}
}

func (r *Runner) snapshot() point {
	p := point{
		at:               time.Now(),
		phase:            phase(r.phase.Load()),
		filled:           r.ring.Len(),
		inflight:         r.inflight.Load(),
		notStarted:       r.notStarted.Load(),
		warmupNotStarted: r.warmupNotStarted.Load(),
	}
	for i := range r.locals {
		warm, write, read := r.locals[i].snap()
		p.warmup.add(warm)
		p.write.add(write)
		p.read.add(read)
	}
	return p
}

func (r *Runner) printDelta(prev, cur point) {
	dt := cur.at.Sub(prev.at).Seconds()
	if dt <= 0 {
		return
	}
	elapsed := cur.at.Sub(r.started).Seconds()
	warmN := cur.warmup.count - prev.warmup.count
	if warmN > 0 || cur.phase == phaseWarmup {
		r.log.Printf("phase=warmup elapsed=%.1fs filled=%d/%d write_rps=%.1f write_err=%d not_started=%d inflight=%d",
			elapsed,
			cur.filled,
			r.cfg.WarmupSlugs,
			float64(warmN)/dt,
			cur.warmup.errs-prev.warmup.errs,
			cur.warmupNotStarted-prev.warmupNotStarted,
			cur.inflight,
		)
	}
	if cur.phase != phaseMeasure {
		return
	}
	wLat := subHist(cur.write.lat, prev.write.lat)
	rLat := subHist(cur.read.lat, prev.read.lat)
	r.log.Printf("phase=measure elapsed=%.1fs filled=%d write_rps=%.1f read_rps=%.1f write_p50=%s write_p99=%s read_p50=%s read_p99=%s write_err=%d read_err=%d not_started=%d inflight=%d",
		elapsed,
		cur.filled,
		float64(cur.write.count-prev.write.count)/dt,
		float64(cur.read.count-prev.read.count)/dt,
		fmtMS(wLat.quantile(0.50)),
		fmtMS(wLat.quantile(0.99)),
		fmtMS(rLat.quantile(0.50)),
		fmtMS(rLat.quantile(0.99)),
		cur.write.errs-prev.write.errs,
		cur.read.errs-prev.read.errs,
		cur.notStarted-prev.notStarted,
		cur.inflight,
	)
}

func (r *Runner) summary() Summary {
	var warmup, write, read opStats
	for i := range r.locals {
		r.locals[i].mu.Lock()
		warmup.add(r.locals[i].warmup)
		write.add(r.locals[i].write)
		read.add(r.locals[i].read)
		r.locals[i].mu.Unlock()
	}
	statuses := make(map[string]uint64, len(write.statuses)+len(read.statuses))
	for k, v := range write.statuses {
		statuses[k] = v
	}
	for k, v := range read.statuses {
		statuses[k] = v
	}
	return Summary{
		GRPCAddr:         r.cfg.GRPCAddr,
		HTTPBase:         r.cfg.HTTPBase,
		TargetRPS:        r.cfg.RPS,
		WritePercent:     r.cfg.WritePercent,
		CompanyPool:      r.cfg.CompanyPool,
		LongURLBytes:     r.cfg.LongURLBytes,
		ReadDistribution: r.cfg.ReadDistribution,
		Seed:             r.cfg.Seed,
		WarmupSlugs:      r.cfg.WarmupSlugs,
		WarmupSeconds:    round(r.warmup.Seconds(), 3),
		WindowSeconds:    round(r.window.Seconds(), 3),
		ScheduledWrites:  r.scheduledWrites.Load(),
		ScheduledReads:   r.scheduledReads.Load(),
		NotStarted:       r.notStarted.Load(),
		WarmupWrites:     warmup.count,
		WarmupErrors:     warmup.errs,
		WarmupNotStarted: r.warmupNotStarted.Load(),
		Write:            latencyOf(write, r.window),
		Read:             latencyOf(read, r.window),
		Statuses:         statuses,
	}
}
