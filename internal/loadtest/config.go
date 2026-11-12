// Package loadtest is an open-loop driver for short-URL creates and clicks.
//
// Creates call UrlShortnerService.CreateShortUrl and keep the returned slug.
// Clicks GET /{slug} on the redirect port and stop at the 302, so the generated
// long URL is never fetched. The write percentage is exact across every 100
// measured requests.
package loadtest

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	maxRPS          = 200_000
	maxLongURLBytes = 8192
	maxCompanyPool  = 2_000_000
	maxRingSize     = 2_000_000
	maxWorkers      = 100_000
	maxGRPCConns    = 10_000
)

// Config is the load-test command surface.
type Config struct {
	RPS              int
	Duration         time.Duration
	WritePercent     int
	GRPCAddr         string
	HTTPBase         string
	CompanyPool      int
	LongURLBytes     int
	WarmupSlugs      int
	WarmupTimeout    time.Duration
	Workers          int
	GRPCConns        int
	HTTPConns        int
	RingSize         int
	Seed             int64
	ReadDistribution string
	Timeout          time.Duration
	ReportInterval   time.Duration
	Channel          int
	IsAdvc           int
}

// DefaultConfig matches a 15k run at 80% writes against a local server.
func DefaultConfig() Config {
	return Config{
		RPS:              15000,
		Duration:         10 * time.Minute,
		WritePercent:     80,
		GRPCAddr:         "127.0.0.1:8080",
		HTTPBase:         "http://127.0.0.1:8001",
		CompanyPool:      1000,
		LongURLBytes:     160,
		WarmupSlugs:      20000,
		Workers:          512,
		GRPCConns:        64,
		HTTPConns:        512,
		RingSize:         100000,
		Seed:             1,
		ReadDistribution: "uniform",
		Timeout:          3 * time.Second,
		ReportInterval:   5 * time.Second,
		Channel:          1,
		IsAdvc:           0,
	}
}

// EffectiveWarmupTimeout is the deadline for filling the slug ring.
// A zero WarmupTimeout scales with warmup-slugs / rps and stays at least 30s.
func (c Config) EffectiveWarmupTimeout() time.Duration {
	if c.WarmupTimeout > 0 {
		return c.WarmupTimeout
	}
	if c.RPS <= 0 {
		return 30 * time.Second
	}
	d := time.Duration(float64(c.WarmupSlugs) / float64(c.RPS) * 3 * float64(time.Second))
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// Validate checks flag combinations before any sockets are opened.
func (c Config) Validate() error {
	if c.RPS <= 0 || c.RPS > maxRPS {
		return fmt.Errorf("rps must be between 1 and %d", maxRPS)
	}
	if c.Duration < time.Second {
		return errors.New("duration must be at least 1s, for example 30s or 10m")
	}
	if c.WritePercent < 1 || c.WritePercent > 100 {
		return errors.New("write-percent must be between 1 and 100")
	}
	if c.WritePercent < 100 && c.WarmupSlugs < 1 {
		return errors.New("warmup-slugs must be at least 1 when write-percent is below 100")
	}
	if c.WarmupSlugs < 0 {
		return errors.New("warmup-slugs must be >= 0")
	}
	if c.WarmupTimeout < 0 {
		return errors.New("warmup-timeout must be >= 0 (0 derives a deadline from rps and warmup-slugs)")
	}
	if c.WarmupTimeout > 0 && c.WarmupTimeout < time.Second {
		return errors.New("warmup-timeout must be at least 1s")
	}
	if strings.TrimSpace(c.GRPCAddr) == "" || strings.Contains(c.GRPCAddr, "://") {
		return fmt.Errorf("grpc-addr must be host:port, got %q", c.GRPCAddr)
	}
	u, err := url.Parse(c.HTTPBase)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("http-base must be an absolute http(s) URL, got %q", c.HTTPBase)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("http-base must not include a path, query, or fragment, got %q", c.HTTPBase)
	}
	if c.CompanyPool < 1 || c.CompanyPool > maxCompanyPool {
		return fmt.Errorf("company-pool must be between 1 and %d", maxCompanyPool)
	}
	minURL := len(longURLPrefix) + 1
	if c.LongURLBytes < minURL || c.LongURLBytes > maxLongURLBytes {
		return fmt.Errorf("long-url-bytes must be between %d and %d", minURL, maxLongURLBytes)
	}
	if c.Workers < 1 || c.Workers > maxWorkers {
		return fmt.Errorf("workers must be between 1 and %d", maxWorkers)
	}
	if c.GRPCConns < 1 || c.GRPCConns > maxGRPCConns {
		return fmt.Errorf("grpc-conns must be between 1 and %d", maxGRPCConns)
	}
	if c.HTTPConns < 1 || c.HTTPConns > maxWorkers {
		return fmt.Errorf("http-conns must be between 1 and %d", maxWorkers)
	}
	if c.RingSize < 1 || c.RingSize > maxRingSize {
		return fmt.Errorf("ring-size must be between 1 and %d", maxRingSize)
	}
	if c.WarmupSlugs > c.RingSize {
		return fmt.Errorf("warmup-slugs (%d) must be <= ring-size (%d)", c.WarmupSlugs, c.RingSize)
	}
	switch c.ReadDistribution {
	case "uniform", "zipf":
	default:
		return fmt.Errorf("read-distribution must be uniform or zipf, got %q", c.ReadDistribution)
	}
	if c.Timeout < 10*time.Millisecond || c.Timeout > time.Minute {
		return errors.New("timeout must be between 10ms and 1m")
	}
	if c.ReportInterval < 100*time.Millisecond {
		return errors.New("report-interval must be at least 100ms")
	}
	if c.Channel < 0 || c.Channel > 32767 || c.IsAdvc < 0 || c.IsAdvc > 32767 {
		return errors.New("channel and is-advc must be between 0 and 32767")
	}
	return nil
}
