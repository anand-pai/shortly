package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"shortly/internal/loadtest"
)

func main() {
	cfg := loadtest.DefaultConfig()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: loadtest [flags]

Open-loop load for short-URL creates and clicks.
Each create calls gRPC CreateShortUrl with an empty slug, so the service generates it.
Clicks GET /{slug} on the redirect port and stop at the 302.

--write-percent 80 sends 80 creates and 20 clicks in every 100 measured requests.
Warmup is creates only, until --warmup-slugs are stored.
company-pool is the number of distinct company_id values. A small pool hot-spots
company_urls_* partitions; a large pool spreads them. Click lookups only touch url_mappings.

Progress lines go to stderr. The JSON summary goes to stdout.

Examples:
  loadtest --rps 15000 --write-percent 80 --duration 10m --company-pool 1000
  loadtest --rps 15000 --write-percent 60 --duration 10m --company-pool 100000
  loadtest --rps 50000 --write-percent 80 --duration 10m --company-pool 1000 --workers 2000 --grpc-conns 128 --http-conns 2000

`)
		flag.PrintDefaults()
	}
	flag.IntVar(&cfg.RPS, "rps", cfg.RPS, "total create+click rate")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "measured window after warmup, for example 10m")
	flag.IntVar(&cfg.WritePercent, "write-percent", cfg.WritePercent, "percent of measured requests that create; the rest click (80 or 60)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "plaintext gRPC host:port of CreateShortUrl")
	flag.StringVar(&cfg.HTTPBase, "http-base", cfg.HTTPBase, "redirect server base URL, for example http://127.0.0.1:8001")
	flag.IntVar(&cfg.CompanyPool, "company-pool", cfg.CompanyPool, "distinct company_id values, drawn uniformly")
	flag.IntVar(&cfg.LongURLBytes, "long-url-bytes", cfg.LongURLBytes, "random https long URL size in bytes")
	flag.IntVar(&cfg.WarmupSlugs, "warmup-slugs", cfg.WarmupSlugs, "creates stored before clicks start")
	flag.DurationVar(&cfg.WarmupTimeout, "warmup-timeout", cfg.WarmupTimeout, "warmup deadline; 0 derives one from rps and warmup-slugs")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "in-flight requests; raise with rps if latency is more than a few milliseconds")
	flag.IntVar(&cfg.GRPCConns, "grpc-conns", cfg.GRPCConns, "gRPC connections, spread across server pods")
	flag.IntVar(&cfg.HTTPConns, "http-conns", cfg.HTTPConns, "max HTTP connections to the redirect port")
	flag.IntVar(&cfg.RingSize, "ring-size", cfg.RingSize, "recent slugs kept for clicks; oldest is overwritten")
	flag.Int64Var(&cfg.Seed, "seed", cfg.Seed, "RNG seed for URLs and company picks")
	flag.StringVar(&cfg.ReadDistribution, "read-distribution", cfg.ReadDistribution, "uniform or zipf over the slug ring")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "per-request deadline")
	flag.DurationVar(&cfg.ReportInterval, "report-interval", cfg.ReportInterval, "progress line interval")
	flag.IntVar(&cfg.Channel, "channel", cfg.Channel, "create request channel")
	flag.IntVar(&cfg.IsAdvc, "is-advc", cfg.IsAdvc, "create request is_advc")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected args: %s\n", strings.Join(flag.Args(), " "))
		flag.Usage()
		os.Exit(2)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "loadtest rps=%d write_percent=%d duration=%s company_pool=%d distribution=%s grpc=%s http=%s workers=%d grpc_conns=%d http_conns=%d\n",
		cfg.RPS, cfg.WritePercent, cfg.Duration, cfg.CompanyPool, cfg.ReadDistribution, cfg.GRPCAddr, cfg.HTTPBase, cfg.Workers, cfg.GRPCConns, cfg.HTTPConns)
	if cfg.RPS/50 > 0 && cfg.Workers < cfg.RPS/50 {
		fmt.Fprintf(os.Stderr, "warning: %d workers may not hold %d rps once latency exceeds about 20ms; raise --workers and --http-conns\n", cfg.Workers, cfg.RPS)
	}
	if cfg.HTTPConns < cfg.Workers {
		fmt.Fprintf(os.Stderr, "warning: http-conns %d is below workers %d; clicks can queue in the HTTP client\n", cfg.HTTPConns, cfg.Workers)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clients, err := loadtest.Dial(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(1)
	}
	defer clients.Close()
	if err := clients.Probe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(1)
	}

	runner, err := loadtest.New(cfg, clients.Creator, clients.Clicker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", err)
		os.Exit(2)
	}
	summary, runErr := runner.Run(ctx)
	summary.WriteReport(os.Stderr)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: write summary: %v\n", err)
		os.Exit(1)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "loadtest: %v\n", runErr)
		os.Exit(1)
	}
}
