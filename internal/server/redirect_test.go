package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"shortly/internal/repository"
	"shortly/models"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

type fakeTxtlyRepo struct {
	mapping  models.UrlMapping
	shortUrl models.ShortUrl
	err      error
	calls    int
}

func (f *fakeTxtlyRepo) GetShortURL(context.Context, string) (models.UrlMapping, error) {
	f.calls++
	return f.mapping, f.err
}

func (f *fakeTxtlyRepo) CreateShortURL(context.Context, models.ShortUrl) (models.ShortUrl, error) {
	f.calls++
	return f.shortUrl, f.err
}

func TestRedirectHandler(t *testing.T) {
	log := zap.NewNop()

	t.Run("known slug redirects and records metrics", func(t *testing.T) {
		lookup := &fakeTxtlyRepo{mapping: models.UrlMapping{LongUrl: "https://example.com/landing"}}
		handler := newRedirectHandler(lookup, log)

		beforeRequests := testutil.ToFloat64(redirectRequests.WithLabelValues("302", "get"))

		req := httptest.NewRequest(http.MethodGet, "/Ab12Cd34", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusFound)
		}
		if got := rec.Header().Get("Location"); got != "https://example.com/landing" {
			t.Fatalf("location = %q", got)
		}
		if lookup.calls != 1 {
			t.Fatalf("lookup calls = %d, want 1", lookup.calls)
		}
		if got := testutil.ToFloat64(redirectRequests.WithLabelValues("302", "get")); got != beforeRequests+1 {
			t.Fatalf("requests = %v, want %v", got, beforeRequests+1)
		}
	})

	t.Run("missing slug is a not found", func(t *testing.T) {
		lookup := &fakeTxtlyRepo{err: repository.ErrSlugNotFound}
		handler := newRedirectHandler(lookup, log)

		before := testutil.ToFloat64(redirectRequests.WithLabelValues("404", "get"))
		req := httptest.NewRequest(http.MethodGet, "/Ab12Cd34", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
		if got := testutil.ToFloat64(redirectRequests.WithLabelValues("404", "get")); got != before+1 {
			t.Fatalf("requests = %v, want %v", got, before+1)
		}
	})

	t.Run("invalid slug skips the lookup", func(t *testing.T) {
		lookup := &fakeTxtlyRepo{err: errors.New("should not be called")}
		handler := newRedirectHandler(lookup, log)

		req := httptest.NewRequest(http.MethodGet, "/not-a-slug", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
		if lookup.calls != 0 {
			t.Fatalf("lookup calls = %d, want 0", lookup.calls)
		}
	})

	t.Run("non http target is rejected", func(t *testing.T) {
		lookup := &fakeTxtlyRepo{mapping: models.UrlMapping{LongUrl: "javascript:alert(1)"}}
		handler := newRedirectHandler(lookup, log)

		before := testutil.ToFloat64(redirectRequests.WithLabelValues("500", "get"))
		req := httptest.NewRequest(http.MethodGet, "/Ab12Cd34", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
		if got := testutil.ToFloat64(redirectRequests.WithLabelValues("500", "get")); got != before+1 {
			t.Fatalf("requests = %v, want %v", got, before+1)
		}
	})
}
