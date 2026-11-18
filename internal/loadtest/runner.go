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

func spread(i, percent int) bool {
	// true for exactly `percent` values of i in 0..99
	return ((i+1)*percent)/100 != (i*percent)/100
}

// planSlot is the measured-phase schedule when sms-percent is set.
// Over every 100 requests, exactly writePercent slots are writes and exactly
// smsPercent slots are SMS (channel 0). Writes are allotted inside each channel,
// so a channel does not absorb every click. The two spreads must not share an
// index: that lines the ticks up and, for example, makes an 80/60 mix all SMS
// writes and zero SMS clicks.
func planSlot(seq uint64, writePercent, smsPercent int) (int, int) {
	p := cachedPlan(writePercent, smsPercent)[seq%100]
	return int(p.kind), p.channel
}

type planned struct {
	kind    kind
	channel int
}

var (
	planMu    sync.Mutex
	planCache = map[[2]int][]planned{}
)

func cachedPlan(writePercent, smsPercent int) []planned {
	key := [2]int{writePercent, smsPercent}
	planMu.Lock()
	defer planMu.Unlock()
	if p, ok := planCache[key]; ok {
		return p
	}
	p := buildPlan(writePercent, smsPercent)
	planCache[key] = p
	return p
}

func buildPlan(writePercent, smsPercent int) []planned {
	smsN := smsPercent
	rcsN := 100 - smsN
	smsW, rcsW := channelWriteCounts(writePercent, smsN, rcsN)
	smsMark := writeMarks(smsN, smsW)
	rcsMark := writeMarks(rcsN, rcsW)
	out := make([]planned, 100)
	si, ri := 0, 0
	for i := 0; i < 100; i++ {
		if spread(i, smsPercent) {
			k := kindRead
			if smsMark[si] {
				k = kindWrite
			}
			out[i] = planned{kind: k, channel: 0}
			si++
			continue
		}
		k := kindRead
		if rcsMark[ri] {
			k = kindWrite
		}
		out[i] = planned{kind: k, channel: 1}
		ri++
	}
	return out
}

// channelWriteCounts splits writePercent across the two channel quotas.
// Truncation leftover (at most one, because the quotas sum to 100) goes to the
// channel with the larger remainder.
func channelWriteCounts(writePercent, smsN, rcsN int) (smsW, rcsW int) {
	if smsN > 0 {
		smsW = smsN * writePercent / 100
	}
	if rcsN > 0 {
		rcsW = rcsN * writePercent / 100
	}
	left := writePercent - smsW - rcsW
	if left <= 0 {
		return smsW, rcsW
	}
	smsRem, rcsRem := 0, 0
	if smsN > 0 {
		smsRem = smsN * writePercent % 100
	}
	if rcsN > 0 {
		rcsRem = rcsN * writePercent % 100
	}
	giveSMS := smsN > smsW && (rcsN == rcsW || smsRem > rcsRem || (smsRem == rcsRem && smsN >= rcsN))
	if giveSMS {
		smsW++
	} else if rcsW < rcsN {
		rcsW++
	}
	return smsW, rcsW
}

// writeMarks is true for exactly want indexes in 0..n-1, spread across the range.
func writeMarks(n, want int) []bool {
	out := make([]bool, n)
	if n == 0 || want <= 0 {
		return out
	}
	if want >= n {
		for i := range out {
			out[i] = true
		}
		return out
	}
	for i := 0; i < n; i++ {
		if ((i+1)*want)/n != (i*want)/n {
			out[i] = true
		}
	}
	return out
}

type job struct {
	kind    kind
	phase   phase
	channel int32
	track   bool
}

// Creator issues one short-URL create. The slug field is left empty.
// channel is sent on CreateUrlRequest (0 SMS, 1 RCS/WA, or --channel in legacy mode).
type Creator interface {
	Create(ctx context.Context, longURL, companyID string, channel int32) (string, error)
}

// Clicker requests one slug and returns the HTTP status without following redirects.
type Clicker interface {
	Click(ctx context.Context, slug string) (statusCode int, err error)
}

// Runner paces creates and clicks and collects the measured window.
// A Runner is used for one Run.
type Runner struct {
	cfg       Config
	ring      *Ring    // legacy single pool
	rings     [2]*Ring // split: 0 SMS, 1 RCS/WA; each has capacity RingSize
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
	scheduledSMS     atomic.Uint64
	scheduledRCS     atomic.Uint64
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
	rn := &Runner{
		cfg:       cfg,
		companies: companyIDs(cfg.CompanyPool),
		locals:    make([]localStats, cfg.Workers),
		create:    create,
		click:     click,
		log:       log.New(os.Stderr, "", 0),
	}
	if cfg.SMSPercent < 0 {
		rn.ring = newRing(cfg.RingSize)
	} else {
		rn.rings[0] = newRing(cfg.RingSize)
		rn.rings[1] = newRing(cfg.RingSize)
	}
	return rn, nil
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
	smsCount         uint64
	rcsCount         uint64
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
	if r.cfg.ReadDistribution == "zipf" && r.ring != nil && r.ring.Cap() > 1 {
		zipf = rand.NewZipf(rng, 1.2, 1, uint64(r.ring.Cap()-1))
	}
	var zipfs [2]*rand.Zipf
	if r.cfg.ReadDistribution == "zipf" {
		for i, ring := range r.rings {
			if ring != nil && ring.Cap() > 1 {
				zipfs[i] = rand.NewZipf(rng, 1.2, 1, uint64(ring.Cap()-1))
			}
		}
	}
	for job := range jobs {
		r.inflight.Add(1)
		switch job.kind {
		case kindRead:
			z := zipf
			if r.cfg.SMSPercent >= 0 {
				z = zipfs[ringIndexFor(job.channel)]
			}
			r.doRead(id, job, rng, z)
		default:
			r.doWrite(id, job, rng, urls)
		}
		r.inflight.Add(-1)
	}
}

func ringIndexFor(channel int32) int {
	if channel == 0 {
		return 0
	}
	return 1
}

func (r *Runner) slugRing(channel int32) *Ring {
	if r.cfg.SMSPercent < 0 {
		return r.ring
	}
	return r.rings[ringIndexFor(channel)]
}

func (r *Runner) doWrite(id int, job job, rng *rand.Rand, urls *urlGen) {
	company := r.companies[rng.Intn(len(r.companies))]
	longURL := urls.next(rng)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	start := time.Now()
	slug, err := r.create.Create(ctx, longURL, company, job.channel)
	elapsed := time.Since(start)
	cancel()
	if err != nil {
		key := "write:" + status.Code(err).String()
		if errors.Is(err, errBadSlug) {
			key = "write:BadSlug"
		}
		r.locals[id].record(job.phase, kindWrite, job.channel, job.track, elapsed, key, false, true)
		return
	}
	r.slugRing(job.channel).Push(slug)
	r.locals[id].record(job.phase, kindWrite, job.channel, job.track, elapsed, "write:OK", true, true)
}

func (r *Runner) doRead(id int, job job, rng *rand.Rand, zipf *rand.Zipf) {
	slug, ok := r.slugRing(job.channel).Sample(rng, zipf)
	if !ok || !slugPattern.MatchString(slug) {
		key := "read:empty_pool"
		if ok {
			key = "read:BadSlug"
		}
		r.locals[id].record(job.phase, kindRead, job.channel, job.track, 0, key, false, false)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	start := time.Now()
	code, err := r.click.Click(ctx, slug)
	elapsed := time.Since(start)
	cancel()
	if err != nil {
		r.locals[id].record(job.phase, kindRead, job.channel, job.track, elapsed, "read:"+netClass(err), false, true)
		return
	}
	r.locals[id].record(job.phase, kindRead, job.channel, job.track, elapsed, "read:"+strconv.Itoa(code), code == http.StatusFound, true)
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
		acc       float64
		seq       uint64
		warmupSeq uint64
		last      = time.Now()
		runErr    error
	)

loop:
	for {
		if err := ctx.Err(); err != nil {
			runErr = err
			break
		}
		if measureStart.IsZero() && r.warmupMet() {
			measureStart = time.Now()
			last = measureStart
			acc = 0
			r.phase.Store(int32(phaseMeasure))
			if r.cfg.SMSPercent < 0 {
				r.log.Printf("warmup complete slugs=%d warmup=%s", r.ring.Len(), measureStart.Sub(r.started).Round(time.Millisecond))
			} else {
				r.log.Printf("warmup complete sms=%d rcs_wa=%d warmup=%s", r.rings[0].Len(), r.rings[1].Len(), measureStart.Sub(r.started).Round(time.Millisecond))
			}
		}
		if measureStart.IsZero() && time.Now().After(warmupDeadline) {
			if r.cfg.SMSPercent < 0 {
				runErr = fmt.Errorf("warmup timed out with %d/%d slugs stored", r.ring.Len(), r.cfg.WarmupSlugs)
			} else {
				smsTarget, rcsTarget := channelTargets(r.cfg.WarmupSlugs, r.cfg.SMSPercent)
				runErr = fmt.Errorf("warmup timed out with sms %d/%d rcs_wa %d/%d slugs stored",
					r.rings[0].Len(), smsTarget, r.rings[1].Len(), rcsTarget)
			}
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
			var j job
			if phaseNow == phaseMeasure {
				j = r.nextMeasured(&seq)
			} else {
				j = r.nextWarmup(&warmupSeq)
			}
			select {
			case jobs <- j:
			default:
				if phaseNow == phaseMeasure {
					r.notStarted.Add(1)
				} else {
					if r.cfg.SMSPercent >= 0 {
						warmupSeq--
					}
					r.warmupNotStarted.Add(1)
				}
			}
		}
	}
	return runErr
}

func channelTargets(warmupSlugs, smsPercent int) (sms, rcs int) {
	sms = warmupSlugs * smsPercent / 100
	rcs = warmupSlugs - sms
	return sms, rcs
}

func (r *Runner) warmupMet() bool {
	if r.cfg.SMSPercent < 0 {
		return r.ring.Len() >= r.cfg.WarmupSlugs
	}
	smsTarget, rcsTarget := channelTargets(r.cfg.WarmupSlugs, r.cfg.SMSPercent)
	if smsTarget > 0 && r.rings[0].Len() < smsTarget {
		return false
	}
	if rcsTarget > 0 && r.rings[1].Len() < rcsTarget {
		return false
	}
	return true
}

func (r *Runner) stored() int {
	if r.cfg.SMSPercent < 0 {
		return r.ring.Len()
	}
	return r.rings[0].Len() + r.rings[1].Len()
}

// nextWarmup is creates only. A split run takes the channel from spread on the
// sequence of queued creates, so the mix matches sms-percent.
func (r *Runner) nextWarmup(seq *uint64) job {
	j := job{kind: kindWrite, phase: phaseWarmup, channel: int32(r.cfg.Channel)}
	if r.cfg.SMSPercent < 0 {
		j.track = j.channel == 0 || j.channel == 1
		return j
	}
	j.channel = 1
	if spread(int(*seq%100), r.cfg.SMSPercent) {
		j.channel = 0
	}
	*seq++
	j.track = true
	return j
}

func (r *Runner) nextMeasured(seq *uint64) job {
	j := job{phase: phaseMeasure}
	if r.cfg.SMSPercent < 0 {
		j.kind = kindFor(*seq, r.cfg.WritePercent)
		*seq++
		if j.kind == kindWrite {
			r.scheduledWrites.Add(1)
			j.channel = int32(r.cfg.Channel)
			j.track = j.channel == 0 || j.channel == 1
		} else {
			r.scheduledReads.Add(1)
		}
		return j
	}
	k, ch := planSlot(*seq, r.cfg.WritePercent, r.cfg.SMSPercent)
	*seq++
	j.kind = kind(k)
	j.channel = int32(ch)
	j.track = true
	if j.kind == kindWrite {
		r.scheduledWrites.Add(1)
	} else {
		r.scheduledReads.Add(1)
	}
	if ch == 0 {
		r.scheduledSMS.Add(1)
	} else {
		r.scheduledRCS.Add(1)
	}
	return j
}

func (r *Runner) dropToken(ph phase, seq *uint64) {
	if ph != phaseMeasure {
		r.warmupNotStarted.Add(1)
		return
	}
	r.nextMeasured(seq)
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
		filled:           r.stored(),
		inflight:         r.inflight.Load(),
		notStarted:       r.notStarted.Load(),
		warmupNotStarted: r.warmupNotStarted.Load(),
	}
	for i := range r.locals {
		warm, write, read, smsN, rcsN := r.locals[i].snap()
		p.warmup.add(warm)
		p.write.add(write)
		p.read.add(read)
		p.smsCount += smsN
		p.rcsCount += rcsN
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
	if r.cfg.SMSPercent >= 0 {
		r.log.Printf("phase=measure sms_rps=%.1f rcs_rps=%.1f",
			float64(cur.smsCount-prev.smsCount)/dt,
			float64(cur.rcsCount-prev.rcsCount)/dt,
		)
	}
}

func (r *Runner) summary() Summary {
	var warmup, write, read opStats
	var smsWrite, smsRead, rcsWrite, rcsRead opStats
	for i := range r.locals {
		r.locals[i].mu.Lock()
		warmup.add(r.locals[i].warmup)
		write.add(r.locals[i].write)
		read.add(r.locals[i].read)
		smsWrite.add(r.locals[i].chWrite[0])
		smsRead.add(r.locals[i].chRead[0])
		rcsWrite.add(r.locals[i].chWrite[1])
		rcsRead.add(r.locals[i].chRead[1])
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
		SMSPercent:       r.cfg.SMSPercent,
		CompanyPool:      r.cfg.CompanyPool,
		LongURLBytes:     r.cfg.LongURLBytes,
		ReadDistribution: r.cfg.ReadDistribution,
		Seed:             r.cfg.Seed,
		WarmupSlugs:      r.cfg.WarmupSlugs,
		WarmupSeconds:    round(r.warmup.Seconds(), 3),
		WindowSeconds:    round(r.window.Seconds(), 3),
		ScheduledWrites:  r.scheduledWrites.Load(),
		ScheduledReads:   r.scheduledReads.Load(),
		ScheduledSMS:     r.scheduledSMS.Load(),
		ScheduledRCS:     r.scheduledRCS.Load(),
		NotStarted:       r.notStarted.Load(),
		WarmupWrites:     warmup.count,
		WarmupErrors:     warmup.errs,
		WarmupNotStarted: r.warmupNotStarted.Load(),
		Write:            latencyOf(write, r.window),
		Read:             latencyOf(read, r.window),
		SMS: ChannelReport{
			Write: latencyOf(smsWrite, r.window),
			Read:  latencyOf(smsRead, r.window),
		},
		RCSWA: ChannelReport{
			Write: latencyOf(rcsWrite, r.window),
			Read:  latencyOf(rcsRead, r.window),
		},
		Statuses: statuses,
	}
}
