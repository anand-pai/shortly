package loadtest

import (
	"testing"
	"time"
)

func TestHistogramPercentiles(t *testing.T) {
	var h histogram
	for i := 0; i < 100; i++ {
		h.observe(time.Millisecond)
	}
	if got := h.quantile(0.50); got != time.Millisecond {
		t.Fatalf("p50 %s", got)
	}
	if got := h.quantile(0.99); got != time.Millisecond {
		t.Fatalf("p99 %s", got)
	}
	if got := h.mean(); got != time.Millisecond {
		t.Fatalf("mean %s", got)
	}

	var mixed histogram
	mixed.observe(1600 * time.Microsecond)
	if got := mixed.quantile(0.50); got != 2*time.Millisecond {
		t.Fatalf("bucket %s", got)
	}
	if time.Duration(mixed.max) != 1600*time.Microsecond {
		t.Fatalf("max %s", time.Duration(mixed.max))
	}
	mixed.observe(11 * time.Second)
	if got := mixed.quantile(1); got != 11*time.Second {
		t.Fatalf("overflow %s", got)
	}
}

func TestHistogramMergeAndDelta(t *testing.T) {
	var a, b histogram
	a.observe(time.Millisecond)
	a.observe(time.Millisecond)
	b.observe(5 * time.Millisecond)
	prev := a.clone()
	a.merge(b)
	if a.total != 3 {
		t.Fatalf("total %d", a.total)
	}
	delta := subHist(a, prev)
	if delta.total != 1 {
		t.Fatalf("delta %d", delta.total)
	}
	if delta.quantile(0.50) != 5*time.Millisecond {
		t.Fatalf("delta p50 %s", delta.quantile(0.50))
	}
}
