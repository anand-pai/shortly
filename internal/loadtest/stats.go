package loadtest

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"
)

// bucketBounds are upper bounds. A sample is counted in the first bound it
// does not exceed. Percentiles report that upper bound, so they sit at or
// above the true value. Mean and max are exact.
var bucketBounds = []time.Duration{
	100 * time.Microsecond,
	200 * time.Microsecond,
	300 * time.Microsecond,
	500 * time.Microsecond,
	750 * time.Microsecond,
	time.Millisecond,
	1500 * time.Microsecond,
	2 * time.Millisecond,
	3 * time.Millisecond,
	4 * time.Millisecond,
	5 * time.Millisecond,
	7500 * time.Microsecond,
	10 * time.Millisecond,
	15 * time.Millisecond,
	20 * time.Millisecond,
	25 * time.Millisecond,
	30 * time.Millisecond,
	40 * time.Millisecond,
	50 * time.Millisecond,
	75 * time.Millisecond,
	80 * time.Millisecond,
	90 * time.Millisecond,
	100 * time.Millisecond,
	150 * time.Millisecond,
	200 * time.Millisecond,
	250 * time.Millisecond,
	300 * time.Millisecond,
	400 * time.Millisecond,
	500 * time.Millisecond,
	750 * time.Millisecond,
	time.Second,
	1500 * time.Millisecond,
	2 * time.Second,
	3 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

type histogram struct {
	counts []uint64
	total  uint64
	sum    int64
	max    int64
}

func (h *histogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if len(h.counts) == 0 {
		h.counts = make([]uint64, len(bucketBounds)+1)
	}
	h.total++
	h.sum += int64(d)
	if int64(d) > h.max {
		h.max = int64(d)
	}
	i := sort.Search(len(bucketBounds), func(i int) bool { return d <= bucketBounds[i] })
	h.counts[i]++
}

func (h histogram) clone() histogram {
	out := h
	if h.counts != nil {
		out.counts = append([]uint64(nil), h.counts...)
	}
	return out
}

func (h *histogram) merge(o histogram) {
	if o.total == 0 && len(o.counts) == 0 {
		return
	}
	if len(h.counts) == 0 {
		h.counts = make([]uint64, len(bucketBounds)+1)
	}
	for i := range o.counts {
		if i < len(h.counts) {
			h.counts[i] += o.counts[i]
		}
	}
	h.total += o.total
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

func (h histogram) mean() time.Duration {
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.sum / int64(h.total))
}

func (h histogram) quantile(q float64) time.Duration {
	if h.total == 0 || len(h.counts) == 0 {
		return 0
	}
	if q <= 0 {
		return 0
	}
	if q > 1 {
		q = 1
	}
	target := uint64(math.Ceil(q * float64(h.total)))
	if target < 1 {
		target = 1
	}
	if target > h.total {
		target = h.total
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			if i >= len(bucketBounds) {
				if h.max > 0 {
					return time.Duration(h.max)
				}
				return bucketBounds[len(bucketBounds)-1]
			}
			return bucketBounds[i]
		}
	}
	if h.max > 0 {
		return time.Duration(h.max)
	}
	return 0
}

func subHist(cur, prev histogram) histogram {
	out := cur.clone()
	if len(out.counts) == 0 || len(prev.counts) == 0 {
		return out
	}
	for i := range prev.counts {
		if i < len(out.counts) {
			out.counts[i] -= prev.counts[i]
		}
	}
	out.total -= prev.total
	out.sum -= prev.sum
	out.max = 0
	return out
}

type opStats struct {
	count    uint64
	errs     uint64
	lat      histogram
	statuses map[string]uint64
}

func (o *opStats) record(d time.Duration, key string, success, timed bool) {
	o.count++
	if o.statuses == nil {
		o.statuses = make(map[string]uint64)
	}
	o.statuses[key]++
	if !success {
		o.errs++
	}
	if timed {
		o.lat.observe(d)
	}
}

func (o *opStats) add(other opStats) {
	o.count += other.count
	o.errs += other.errs
	o.lat.merge(other.lat)
	if len(other.statuses) == 0 {
		return
	}
	if o.statuses == nil {
		o.statuses = make(map[string]uint64, len(other.statuses))
	}
	for k, v := range other.statuses {
		o.statuses[k] += v
	}
}

func (o opStats) snap() opSnap {
	return opSnap{count: o.count, errs: o.errs, lat: o.lat.clone()}
}

type opSnap struct {
	count uint64
	errs  uint64
	lat   histogram
}

func (s *opSnap) add(o opSnap) {
	s.count += o.count
	s.errs += o.errs
	s.lat.merge(o.lat)
}

type localStats struct {
	mu     sync.Mutex
	warmup opStats
	write  opStats
	read   opStats
}

func (s *localStats) record(ph phase, k kind, d time.Duration, key string, success, timed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dst := &s.write
	switch {
	case ph == phaseWarmup:
		dst = &s.warmup
	case k == kindRead:
		dst = &s.read
	}
	dst.record(d, key, success, timed)
}

func (s *localStats) snap() (warmup, write, read opSnap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.warmup.snap(), s.write.snap(), s.read.snap()
}

// Latency is one side of the measured window (creates or clicks).
// Percentiles are histogram bucket upper bounds. Mean and max are exact.
type Latency struct {
	Count       uint64  `json:"count"`
	AchievedRPS float64 `json:"achieved_rps"`
	Errors      uint64  `json:"errors"`
	ErrorRate   float64 `json:"error_rate"`
	MeanMS      float64 `json:"mean_ms"`
	P50MS       float64 `json:"p50_ms"`
	P95MS       float64 `json:"p95_ms"`
	P99MS       float64 `json:"p99_ms"`
	MaxMS       float64 `json:"max_ms"`
}

// Summary is the measured window, written as JSON on stdout.
type Summary struct {
	GRPCAddr         string            `json:"grpc_addr"`
	HTTPBase         string            `json:"http_base"`
	TargetRPS        int               `json:"target_rps"`
	WritePercent     int               `json:"write_percent"`
	CompanyPool      int               `json:"company_pool"`
	LongURLBytes     int               `json:"long_url_bytes"`
	ReadDistribution string            `json:"read_distribution"`
	Seed             int64             `json:"seed"`
	WarmupSlugs      int               `json:"warmup_slugs"`
	WarmupSeconds    float64           `json:"warmup_seconds"`
	WindowSeconds    float64           `json:"window_seconds"`
	ScheduledWrites  uint64            `json:"scheduled_writes"`
	ScheduledReads   uint64            `json:"scheduled_reads"`
	NotStarted       uint64            `json:"not_started"`
	WarmupWrites     uint64            `json:"warmup_writes"`
	WarmupErrors     uint64            `json:"warmup_errors"`
	WarmupNotStarted uint64            `json:"warmup_not_started"`
	Write            Latency           `json:"write"`
	Read             Latency           `json:"read"`
	Statuses         map[string]uint64 `json:"statuses"`
}

// WriteReport prints the measured window for a terminal.
func (s Summary) WriteReport(w io.Writer) {
	fmt.Fprintf(w, "target %d rps   write %d%%   company_pool %d   distribution %s\n",
		s.TargetRPS, s.WritePercent, s.CompanyPool, s.ReadDistribution)
	fmt.Fprintf(w, "window %.3fs   warmup %.3fs   scheduled_writes %d   scheduled_reads %d   not_started %d\n",
		s.WindowSeconds, s.WarmupSeconds, s.ScheduledWrites, s.ScheduledReads, s.NotStarted)
	writeLatencyLine(w, "write", s.Write)
	writeLatencyLine(w, "read ", s.Read)
	fmt.Fprintln(w, "p50/p95/p99 are histogram bucket upper bounds; mean and max are exact")
}

func writeLatencyLine(w io.Writer, name string, lat Latency) {
	fmt.Fprintf(w, "%s  rps %10.2f  count %d  errors %d (%.6f)  mean %.3fms  p50 %.3fms  p95 %.3fms  p99 %.3fms  max %.3fms\n",
		name, lat.AchievedRPS, lat.Count, lat.Errors, lat.ErrorRate,
		lat.MeanMS, lat.P50MS, lat.P95MS, lat.P99MS, lat.MaxMS)
}

func latencyOf(o opStats, window time.Duration) Latency {
	var rps float64
	if sec := window.Seconds(); sec > 0 {
		rps = float64(o.count) / sec
	}
	var errRate float64
	if o.count > 0 {
		errRate = float64(o.errs) / float64(o.count)
	}
	ms := func(d time.Duration) float64 {
		return round(float64(d)/float64(time.Millisecond), 3)
	}
	return Latency{
		Count:       o.count,
		AchievedRPS: round(rps, 2),
		Errors:      o.errs,
		ErrorRate:   round(errRate, 6),
		MeanMS:      ms(o.lat.mean()),
		P50MS:       ms(o.lat.quantile(0.50)),
		P95MS:       ms(o.lat.quantile(0.95)),
		P99MS:       ms(o.lat.quantile(0.99)),
		MaxMS:       ms(time.Duration(o.lat.max)),
	}
}

func round(v float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(v*p) / p
}

func fmtMS(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}
