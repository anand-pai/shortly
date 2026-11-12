package loadtest

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero rps", func(c *Config) { c.RPS = 0 }},
		{"duration without unit", func(c *Config) { c.Duration = 10 }},
		{"write percent", func(c *Config) { c.WritePercent = 0 }},
		{"grpc url", func(c *Config) { c.GRPCAddr = "http://127.0.0.1:8080" }},
		{"http path", func(c *Config) { c.HTTPBase = "http://127.0.0.1:8001/r" }},
		{"warmup bigger than ring", func(c *Config) { c.WarmupSlugs = 10; c.RingSize = 4 }},
		{"reads without warmup", func(c *Config) { c.WritePercent = 80; c.WarmupSlugs = 0 }},
		{"bad distribution", func(c *Config) { c.ReadDistribution = "hot" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEffectiveWarmupTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RPS = 15000
	cfg.WarmupSlugs = 20000
	if got := cfg.EffectiveWarmupTimeout(); got != 30*time.Second {
		t.Fatalf("floor = %s", got)
	}
	cfg.RPS = 100
	cfg.WarmupSlugs = 20000
	if got := cfg.EffectiveWarmupTimeout(); got != 10*time.Minute {
		t.Fatalf("scaled = %s, want 10m", got)
	}
	cfg.WarmupTimeout = 5 * time.Second
	if got := cfg.EffectiveWarmupTimeout(); got != 5*time.Second {
		t.Fatalf("explicit = %s", got)
	}
}

func TestWriteOnlyAllowsEmptyWarmup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WritePercent = 100
	cfg.WarmupSlugs = 0
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
