package loadtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	gen "shortly/gen/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

const probeSlug = "00000000"

// Clients are the plaintext gRPC and HTTP pools used by a run.
type Clients struct {
	Creator   Creator
	Clicker   Clicker
	conns     []*grpc.ClientConn
	transport *http.Transport
	health    grpc_health_v1.HealthClient
}

// Dial opens the gRPC pool and waits until every connection is ready.
// The gRPC target is plaintext. HTTP_PROXY is ignored so click timings
// stay on the redirect server.
func Dial(ctx context.Context, cfg Config) (*Clients, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableServiceConfig(),
		grpc.WithDisableRetry(),
		grpc.WithInitialWindowSize(1 << 20),
		grpc.WithInitialConnWindowSize(1 << 20),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false)),
	}
	conns := make([]*grpc.ClientConn, 0, cfg.GRPCConns)
	closeConns := func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}
	for i := 0; i < cfg.GRPCConns; i++ {
		conn, err := grpc.NewClient(cfg.GRPCAddr, opts...)
		if err != nil {
			closeConns()
			return nil, fmt.Errorf("grpc dial %s: %w", cfg.GRPCAddr, err)
		}
		conns = append(conns, conn)
		conn.Connect()
	}
	for i, conn := range conns {
		if err := waitReady(ctx, conn); err != nil {
			closeConns()
			return nil, fmt.Errorf("grpc conn %d: %w", i, err)
		}
	}
	callers := make([]gen.UrlShortnerServiceClient, len(conns))
	for i, conn := range conns {
		callers[i] = gen.NewUrlShortnerServiceClient(conn)
	}
	clicker := newHTTPClicker(cfg.HTTPBase, cfg.Timeout, cfg.HTTPConns)
	return &Clients{
		Creator: &grpcCreator{
			clients: callers,
			channel: int32(cfg.Channel),
			isAdvc:  int32(cfg.IsAdvc),
		},
		Clicker:   clicker,
		conns:     conns,
		transport: clicker.transport,
		health:    grpc_health_v1.NewHealthClient(conns[0]),
	}, nil
}

func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(readyCtx, state) {
			return fmt.Errorf("%s not ready (%s)", conn.Target(), conn.GetState())
		}
	}
}

// Probe checks gRPC health and that the redirect port answers a slug GET.
// A 404 is a healthy miss. A 302 means that slug already exists.
func (c *Clients) Probe(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.health.Check(pctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("grpc health: %w", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("grpc health: %s", resp.GetStatus().String())
	}
	code, err := c.Clicker.Click(pctx, probeSlug)
	if err != nil {
		return fmt.Errorf("redirect probe: %w", err)
	}
	if code != http.StatusNotFound && code != http.StatusFound {
		return fmt.Errorf("redirect probe: status %d", code)
	}
	return nil
}

// Close releases the pools.
func (c *Clients) Close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

type grpcCreator struct {
	clients []gen.UrlShortnerServiceClient
	next    atomic.Uint64
	channel int32
	isAdvc  int32
}

func (g *grpcCreator) Create(ctx context.Context, longURL, companyID string) (string, error) {
	i := g.next.Add(1)
	client := g.clients[int(i%uint64(len(g.clients)))]
	resp, err := client.CreateShortUrl(ctx, &gen.CreateUrlRequest{
		LongUrl:   longURL,
		CompanyId: companyID,
		Channel:   g.channel,
		IsAdvc:    g.isAdvc,
	})
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", fmt.Errorf("%w: empty response", errBadSlug)
	}
	return slugFromShortURL(resp.GetShortUrl())
}

type httpClicker struct {
	base      string
	client    *http.Client
	transport *http.Transport
}

func newHTTPClicker(base string, timeout time.Duration, conns int) *httpClicker {
	dialTimeout := timeout
	if dialTimeout > 5*time.Second {
		dialTimeout = 5 * time.Second
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        conns,
		MaxIdleConnsPerHost: conns,
		MaxConnsPerHost:     conns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	return &httpClicker{
		base: strings.TrimRight(base, "/"),
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		transport: transport,
	}
}

func (h *httpClicker) Click(ctx context.Context, slug string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.base+"/"+slug, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "shortly-loadtest")
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

func netClass(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "error"
}
