package loadtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gen "shortly/gen/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpclog "google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) {
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
	os.Exit(m.Run())
}

func TestKindFor(t *testing.T) {
	cases := []struct {
		pct    int
		writes int
	}{
		{60, 600},
		{80, 800},
		{100, 1000},
	}
	for _, tc := range cases {
		writes := 0
		for seq := 0; seq < 1000; seq++ {
			if kindFor(uint64(seq), tc.pct) == kindWrite {
				writes++
			}
		}
		if writes != tc.writes {
			t.Fatalf("percent %d writes %d, want %d", tc.pct, writes, tc.writes)
		}
	}
}

type memCreate struct {
	n        atomic.Uint64
	bad      atomic.Uint64
	pool     int
	urlBytes int
}

func (m *memCreate) Create(_ context.Context, longURL, companyID string) (string, error) {
	parsed, err := urlParseOK(longURL)
	id, idErr := strconv.Atoi(strings.TrimPrefix(companyID, "c-"))
	if err != nil || !parsed || len(longURL) != m.urlBytes || idErr != nil || id < 0 || id >= m.pool {
		m.bad.Add(1)
		return "", errors.New("bad payload")
	}
	return fmt.Sprintf("%08x", m.n.Add(1)), nil
}

func urlParseOK(raw string) (bool, error) {
	if !strings.HasPrefix(raw, longURLPrefix) {
		return false, nil
	}
	return true, nil
}

type memClick struct {
	n atomic.Uint64
}

func (m *memClick) Click(_ context.Context, slug string) (int, error) {
	if !slugPattern.MatchString(slug) {
		return 0, errors.New("bad slug")
	}
	m.n.Add(1)
	return http.StatusFound, nil
}

func TestRunnerMix(t *testing.T) {
	for _, dist := range []string{"uniform", "zipf"} {
		t.Run(dist, func(t *testing.T) {
			cfg := fakeConfig(dist)
			create := &memCreate{pool: cfg.CompanyPool, urlBytes: cfg.LongURLBytes}
			click := &memClick{}
			sum := runCfg(t, cfg, create, click)
			if create.bad.Load() != 0 {
				t.Fatalf("bad payloads %d", create.bad.Load())
			}
			if create.n.Load() != sum.WarmupWrites+sum.Write.Count {
				t.Fatalf("creates %d warmup %d writes %d", create.n.Load(), sum.WarmupWrites, sum.Write.Count)
			}
			if click.n.Load() != sum.Read.Count {
				t.Fatalf("clicks %d reads %d", click.n.Load(), sum.Read.Count)
			}
		})
	}
}

func TestWarmupTimeout(t *testing.T) {
	cfg := fakeConfig("uniform")
	cfg.RPS = 200
	cfg.WarmupSlugs = 10
	cfg.WarmupTimeout = time.Second
	cfg.Workers = 4
	sum := runCfg(t, cfg, failCreate{}, &memClick{})
	if sum.WarmupErrors == 0 || sum.WarmupWrites != sum.WarmupErrors {
		t.Fatalf("warmup writes %d errors %d", sum.WarmupWrites, sum.WarmupErrors)
	}
	if sum.WindowSeconds != 0 {
		t.Fatalf("window %f", sum.WindowSeconds)
	}
	if sum.Read.Count != 0 {
		t.Fatalf("reads %d", sum.Read.Count)
	}
}

type failCreate struct{}

func (failCreate) Create(context.Context, string, string) (string, error) {
	return "", errors.New("down")
}

func fakeConfig(dist string) Config {
	cfg := DefaultConfig()
	cfg.RPS = 4000
	cfg.Duration = time.Second
	cfg.WritePercent = 80
	cfg.CompanyPool = 4
	cfg.LongURLBytes = 48
	cfg.WarmupSlugs = 40
	cfg.Workers = 32
	cfg.GRPCConns = 1
	cfg.HTTPConns = 32
	cfg.RingSize = 1000
	cfg.ReadDistribution = dist
	cfg.ReportInterval = 250 * time.Millisecond
	cfg.Timeout = 2 * time.Second
	cfg.Seed = 11
	return cfg
}

func runCfg(t *testing.T, cfg Config, create Creator, click Clicker) Summary {
	t.Helper()
	runner, err := New(cfg, create, click)
	if err != nil {
		t.Fatal(err)
	}
	runner.log = log.New(io.Discard, "", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sum, err := runner.Run(ctx)
	if cfg.WarmupTimeout > 0 && cfg.WarmupSlugs > 0 && createFails(create) {
		if err == nil || !strings.Contains(err.Error(), "warmup timed out") {
			t.Fatalf("err %v", err)
		}
		return sum
	}
	if err != nil {
		t.Fatal(err)
	}
	assertSchedule(t, cfg, sum)
	if sum.NotStarted != 0 {
		t.Fatalf("not started %d", sum.NotStarted)
	}
	if sum.Write.Errors != 0 || sum.Read.Errors != 0 {
		t.Fatalf("errors write=%d read=%d statuses=%v", sum.Write.Errors, sum.Read.Errors, sum.Statuses)
	}
	if sum.WarmupWrites < uint64(cfg.WarmupSlugs) {
		t.Fatalf("warmup %d", sum.WarmupWrites)
	}
	if sum.WindowSeconds < 1 || sum.WindowSeconds > 2.5 {
		t.Fatalf("window %f", sum.WindowSeconds)
	}
	if sum.Statuses["write:OK"] != sum.Write.Count || sum.Statuses["read:302"] != sum.Read.Count {
		t.Fatalf("statuses %+v counts write %d read %d", sum.Statuses, sum.Write.Count, sum.Read.Count)
	}
	return sum
}

func createFails(create Creator) bool {
	_, ok := create.(failCreate)
	return ok
}

func assertSchedule(t *testing.T, cfg Config, sum Summary) {
	t.Helper()
	sched := sum.ScheduledWrites + sum.ScheduledReads
	if sched < uint64(cfg.RPS)/2 {
		t.Fatalf("scheduled %d", sched)
	}
	var wantReads uint64
	for i := uint64(0); i < sched; i++ {
		if kindFor(i, cfg.WritePercent) == kindRead {
			wantReads++
		}
	}
	if sum.ScheduledReads != wantReads {
		t.Fatalf("scheduled reads %d, want %d (writes %d)", sum.ScheduledReads, wantReads, sum.ScheduledWrites)
	}
	got := sum.Write.Count + sum.Read.Count + sum.NotStarted
	if got != sched {
		t.Fatalf("completed+not_started=%d scheduled=%d", got, sched)
	}
}

func TestRunnerAgainstServers(t *testing.T) {
	cfg := fakeConfig("uniform")
	cfg.RPS = 600
	cfg.CompanyPool = 6
	cfg.LongURLBytes = 64
	cfg.WarmupSlugs = 30
	cfg.Workers = 24
	cfg.GRPCConns = 4
	cfg.HTTPConns = 24
	cfg.RingSize = 500

	stub := &stubShortener{urls: map[string]string{}, longURLBytes: cfg.LongURLBytes}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	gen.RegisterUrlShortnerServiceServer(gs, stub)
	grpc_health_v1.RegisterHealthServer(gs, health.NewServer())
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slug := strings.Trim(r.URL.Path, "/")
		stub.mu.Lock()
		longURL, ok := stub.urls[slug]
		stub.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, longURL, http.StatusFound)
	}))
	t.Cleanup(httpSrv.Close)

	cfg.GRPCAddr = lis.Addr().String()
	cfg.HTTPBase = httpSrv.URL

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clients, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clients.Close)
	if err := clients.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	sum := runCfg(t, cfg, clients.Creator, clients.Clicker)
	if sum.Statuses["read:302"] != sum.Read.Count {
		t.Fatalf("clicks did not all redirect: %+v", sum.Statuses)
	}
	stub.mu.Lock()
	stored := len(stub.urls)
	stub.mu.Unlock()
	if stored != int(sum.WarmupWrites+sum.Write.Count) {
		t.Fatalf("stored %d creates %d", stored, sum.WarmupWrites+sum.Write.Count)
	}
}

type stubShortener struct {
	gen.UnimplementedUrlShortnerServiceServer
	mu           sync.Mutex
	urls         map[string]string
	n            int
	longURLBytes int
}

func (s *stubShortener) CreateShortUrl(_ context.Context, req *gen.CreateUrlRequest) (*gen.CreateUrlResponse, error) {
	if req.GetSlug() != "" {
		return nil, status.Error(codes.InvalidArgument, "slug must be empty")
	}
	if req.GetChannel() != 1 || req.GetIsAdvc() != 0 {
		return nil, status.Error(codes.InvalidArgument, "channel")
	}
	if len(req.GetLongUrl()) != s.longURLBytes || !strings.HasPrefix(req.GetLongUrl(), longURLPrefix) {
		return nil, status.Error(codes.InvalidArgument, "long url")
	}
	if !strings.HasPrefix(req.GetCompanyId(), "c-") {
		return nil, status.Error(codes.InvalidArgument, "company")
	}
	s.mu.Lock()
	s.n++
	slug := fmt.Sprintf("%08x", s.n)
	s.urls[slug] = req.GetLongUrl()
	s.mu.Unlock()
	return &gen.CreateUrlResponse{ShortUrl: "https://shortly-svc/" + slug}, nil
}
