package loadtest

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestRingIndex(t *testing.T) {
	// Not full: next == n, newest is the last pushed slot.
	if got := ringIndex(2, 2, 0); got != 1 {
		t.Fatalf("newest = %d", got)
	}
	if got := ringIndex(2, 2, 1); got != 0 {
		t.Fatalf("oldest = %d", got)
	}
	// Full, next points at the oldest. Buffer layout from the overwrite test.
	if got := ringIndex(1, 4, 0); got != 0 {
		t.Fatalf("full newest = %d", got)
	}
	if got := ringIndex(1, 4, 1); got != 3 {
		t.Fatalf("full rank1 = %d", got)
	}
}

func TestRingOverwriteOrder(t *testing.T) {
	r := newRing(4)
	for _, s := range []string{"a", "b", "c", "d", "e"} {
		r.Push(s)
	}
	if r.Len() != 4 {
		t.Fatalf("len %d", r.Len())
	}
	var got []string
	for rank := 0; rank < 4; rank++ {
		s, ok := r.sampleRank(rank)
		if !ok {
			t.Fatal("missing")
		}
		got = append(got, s)
	}
	want := []string{"e", "d", "c", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rank %d = %s, want %s (all %v)", i, got[i], want[i], got)
		}
	}
}

func TestRingUniformSample(t *testing.T) {
	r := newRing(8)
	want := map[string]bool{}
	for i := 0; i < 5; i++ {
		s := fmt.Sprintf("%08d", i)
		r.Push(s)
		want[s] = true
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100; i++ {
		got, ok := r.Sample(rng, nil)
		if !ok || !want[got] {
			t.Fatalf("sample %q ok=%v", got, ok)
		}
	}
}

func TestRingZipfSampleStaysInBuffer(t *testing.T) {
	r := newRing(8)
	want := map[string]bool{}
	for i := 0; i < 8; i++ {
		s := fmt.Sprintf("%08d", i)
		r.Push(s)
		want[s] = true
	}
	rng := rand.New(rand.NewSource(2))
	zipf := rand.NewZipf(rng, 1.2, 1, uint64(r.Cap()-1))
	if zipf == nil {
		t.Fatal("zipf generator was not created")
	}
	for i := 0; i < 200; i++ {
		got, ok := r.Sample(rng, zipf)
		if !ok || !want[got] {
			t.Fatalf("sample %q ok=%v", got, ok)
		}
	}
}

func TestRingConcurrent(t *testing.T) {
	r := newRing(64)
	var wg sync.WaitGroup
	for id := 0; id < 8; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id + 1)))
			for n := 0; n < 200; n++ {
				r.Push(fmt.Sprintf("%08d", n))
				r.Sample(rng, nil)
			}
		}(id)
	}
	wg.Wait()
	if r.Len() != 64 {
		t.Fatalf("len %d", r.Len())
	}
}
