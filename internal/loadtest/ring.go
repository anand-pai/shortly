package loadtest

import (
	"math/rand"
	"sync"
)

// Ring is a fixed buffer of recently created slugs.
// Push overwrites the oldest slug once the buffer is full.
// Rank 0 is the newest slug.
type Ring struct {
	mu   sync.RWMutex
	buf  []string
	n    int
	next int
}

func newRing(capacity int) *Ring {
	return &Ring{buf: make([]string, capacity)}
}

func (r *Ring) Cap() int { return len(r.buf) }

func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.n
}

func (r *Ring) Push(slug string) {
	r.mu.Lock()
	r.buf[r.next] = slug
	r.next++
	if r.next == len(r.buf) {
		r.next = 0
	}
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

// Sample returns one stored slug. A non-nil zipf biases toward newer slugs
// (rank 0 is newest). A nil zipf samples uniformly.
func (r *Ring) Sample(rng *rand.Rand, zipf *rand.Zipf) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.n == 0 {
		return "", false
	}
	rank := 0
	switch {
	case r.n == 1:
		rank = 0
	case zipf != nil:
		rank = int(zipf.Uint64())
		if rank < 0 || rank >= r.n {
			rank = r.n - 1
		}
	default:
		rank = rng.Intn(r.n)
	}
	return r.buf[ringIndex(r.next, r.n, rank)], true
}

func (r *Ring) sampleRank(rank int) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.n == 0 {
		return "", false
	}
	if rank < 0 {
		rank = 0
	}
	if rank >= r.n {
		rank = r.n - 1
	}
	return r.buf[ringIndex(r.next, r.n, rank)], true
}

// ringIndex maps a newest-first rank onto the physical buffer.
// When the ring is not full, next == n and rank 0 sits at n-1.
// When it is full, next points at the oldest slot.
func ringIndex(next, n, rank int) int {
	idx := next - 1 - rank
	if idx < 0 {
		idx += n
	}
	return idx
}
