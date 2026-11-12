package loadtest

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickerStopsAtRedirect(t *testing.T) {
	var followed atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Abcd1234":
			http.Redirect(w, r, "/followed", http.StatusFound)
		default:
			followed.Add(1)
			http.Error(w, "followed", http.StatusTeapot)
		}
	}))
	defer srv.Close()

	clicker := newHTTPClicker(srv.URL, 2*time.Second, 2)
	code, err := clicker.Click(t.Context(), "Abcd1234")
	if err != nil {
		t.Fatal(err)
	}
	if code != http.StatusFound {
		t.Fatalf("status %d", code)
	}
	if followed.Load() != 0 {
		t.Fatal("client followed the redirect")
	}
}
