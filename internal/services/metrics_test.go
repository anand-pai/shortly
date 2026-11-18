package services

import (
	"context"
	"errors"
	"testing"

	gen "shortly/gen/proto"
	"shortly/models"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRepo struct {
	err   error
	calls int
}

func (f *fakeRepo) CreateShortURL(ctx context.Context, s models.ShortUrl) (models.ShortUrl, error) {
	f.calls++
	if f.err != nil {
		return models.ShortUrl{}, f.err
	}
	return s, nil
}

func (f *fakeRepo) GetShortURL(context.Context, string) (models.UrlMapping, error) {
	return models.UrlMapping{}, nil
}

func TestCreateMetricsRegistered(t *testing.T) {
	createRequests.WithLabelValues("sms", "OK")
	createDuration.WithLabelValues("sms", "OK")

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	found := map[string]bool{}
	for _, mf := range mfs {
		found[mf.GetName()] = true
	}
	for _, name := range []string{
		"shortly_grpc_create_duration_seconds",
		"shortly_grpc_create_requests_total",
	} {
		if !found[name] {
			t.Errorf("metric %s is not registered on the default gatherer", name)
		}
	}

	wantBuckets := []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	got := histogramBuckets(t, "sms", "OK")
	if len(got) != len(wantBuckets) {
		t.Fatalf("buckets = %v, want %v", got, wantBuckets)
	}
	for i := range wantBuckets {
		if got[i] != wantBuckets[i] {
			t.Fatalf("buckets = %v, want %v", got, wantBuckets)
		}
	}
}

func TestCreateShortUrlMetrics(t *testing.T) {
	t.Run("sms success", func(t *testing.T) {
		repo := &fakeRepo{}
		svc := &UrlShortnerService{txtlyRepo: repo, log: zap.NewNop()}

		beforeSMS := testutil.ToFloat64(createRequests.WithLabelValues("sms", "OK"))
		beforeRCS := testutil.ToFloat64(createRequests.WithLabelValues("rcs_wa", "OK"))
		beforeHist := histogramSampleCount(t, "sms", "OK")

		resp, err := svc.CreateShortUrl(context.Background(), &gen.CreateUrlRequest{
			LongUrl: "https://example.com/sms",
			Slug:    "sms-slug",
			Channel: 0,
		})
		if err != nil {
			t.Fatalf("CreateShortUrl: %v", err)
		}
		if resp.GetShortUrl() == "" {
			t.Fatal("empty short url")
		}
		if repo.calls != 1 {
			t.Fatalf("repo calls = %d, want 1", repo.calls)
		}
		if got := testutil.ToFloat64(createRequests.WithLabelValues("sms", "OK")); got != beforeSMS+1 {
			t.Fatalf("sms/OK requests = %v, want %v", got, beforeSMS+1)
		}
		if got := testutil.ToFloat64(createRequests.WithLabelValues("rcs_wa", "OK")); got != beforeRCS {
			t.Fatalf("rcs_wa/OK requests = %v, want %v", got, beforeRCS)
		}
		if got := histogramSampleCount(t, "sms", "OK"); got != beforeHist+1 {
			t.Fatalf("sms/OK histogram samples = %d, want %d", got, beforeHist+1)
		}
	})

	t.Run("rcs_wa success", func(t *testing.T) {
		repo := &fakeRepo{}
		svc := &UrlShortnerService{txtlyRepo: repo, log: zap.NewNop()}

		before := testutil.ToFloat64(createRequests.WithLabelValues("rcs_wa", "OK"))
		beforeHist := histogramSampleCount(t, "rcs_wa", "OK")

		_, err := svc.CreateShortUrl(context.Background(), &gen.CreateUrlRequest{
			LongUrl: "https://example.com/rcs",
			Slug:    "rcs-slug",
			Channel: 1,
		})
		if err != nil {
			t.Fatalf("CreateShortUrl: %v", err)
		}
		if got := testutil.ToFloat64(createRequests.WithLabelValues("rcs_wa", "OK")); got != before+1 {
			t.Fatalf("rcs_wa/OK requests = %v, want %v", got, before+1)
		}
		if got := histogramSampleCount(t, "rcs_wa", "OK"); got != beforeHist+1 {
			t.Fatalf("rcs_wa/OK histogram samples = %d, want %d", got, beforeHist+1)
		}
	})

	t.Run("invalid channel", func(t *testing.T) {
		repo := &fakeRepo{err: errors.New("should not be called")}
		svc := &UrlShortnerService{txtlyRepo: repo, log: zap.NewNop()}

		before := testutil.ToFloat64(createRequests.WithLabelValues("invalid", "InvalidArgument"))
		beforeHist := histogramSampleCount(t, "invalid", "InvalidArgument")

		_, err := svc.CreateShortUrl(context.Background(), &gen.CreateUrlRequest{
			LongUrl: "https://example.com/bad",
			Slug:    "bad-slug",
			Channel: 2,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
		}
		if repo.calls != 0 {
			t.Fatalf("repo calls = %d, want 0", repo.calls)
		}
		if got := testutil.ToFloat64(createRequests.WithLabelValues("invalid", "InvalidArgument")); got != before+1 {
			t.Fatalf("invalid/InvalidArgument requests = %v, want %v", got, before+1)
		}
		if got := histogramSampleCount(t, "invalid", "InvalidArgument"); got != beforeHist+1 {
			t.Fatalf("invalid/InvalidArgument histogram samples = %d, want %d", got, beforeHist+1)
		}
	})

	t.Run("repo error", func(t *testing.T) {
		repo := &fakeRepo{err: errors.New("db down")}
		svc := &UrlShortnerService{txtlyRepo: repo, log: zap.NewNop()}

		before := testutil.ToFloat64(createRequests.WithLabelValues("sms", "Internal"))
		beforeHist := histogramSampleCount(t, "sms", "Internal")

		_, err := svc.CreateShortUrl(context.Background(), &gen.CreateUrlRequest{
			LongUrl: "https://example.com/sms",
			Slug:    "sms-slug",
			Channel: 0,
		})
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want Internal", status.Code(err))
		}
		if repo.calls != 1 {
			t.Fatalf("repo calls = %d, want 1", repo.calls)
		}
		if got := testutil.ToFloat64(createRequests.WithLabelValues("sms", "Internal")); got != before+1 {
			t.Fatalf("sms/Internal requests = %v, want %v", got, before+1)
		}
		if got := histogramSampleCount(t, "sms", "Internal"); got != beforeHist+1 {
			t.Fatalf("sms/Internal histogram samples = %d, want %d", got, beforeHist+1)
		}
	})
}

func histogramSampleCount(t *testing.T, channel, code string) uint64 {
	t.Helper()
	count, _, ok := createHistogram(t, channel, code)
	if !ok {
		return 0
	}
	return count
}

func histogramBuckets(t *testing.T, channel, code string) []float64 {
	t.Helper()
	_, bounds, ok := createHistogram(t, channel, code)
	if !ok {
		t.Fatalf("histogram %s/%s not gathered", channel, code)
	}
	return bounds
}

func createHistogram(t *testing.T, channel, code string) (uint64, []float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "shortly_grpc_create_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var gotChannel, gotCode string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "channel":
					gotChannel = lp.GetValue()
				case "code":
					gotCode = lp.GetValue()
				}
			}
			if gotChannel != channel || gotCode != code {
				continue
			}
			h := m.GetHistogram()
			if h == nil {
				t.Fatalf("metric %s/%s is not a histogram", channel, code)
			}
			var bounds []float64
			for _, b := range h.GetBucket() {
				if b.GetUpperBound() > 5 {
					continue
				}
				bounds = append(bounds, b.GetUpperBound())
			}
			return h.GetSampleCount(), bounds, true
		}
	}
	return 0, nil, false
}
