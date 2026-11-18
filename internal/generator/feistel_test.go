package generator

import "testing"

func TestToBase62RoundTrip(t *testing.T) {
	sms := []uint64{0, 1, 62, 1<<47 - 1, 1 << 47, (1 << 48) - 1}
	for _, id := range sms {
		slug, err := ToBase62(id, 48)
		if err != nil {
			t.Fatal(err)
		}
		if len(slug) != 8 {
			t.Fatalf("48-bit slug %q len = %d", slug, len(slug))
		}
		if got := decode48(slug); got != id {
			t.Fatalf("48-bit %d encoded to %s decoded to %d", id, slug, got)
		}
	}

	rcs := []uint64{0, 1, 62, (1 << 48) - 1, 1 << 48, ^uint64(0)}
	for _, id := range rcs {
		slug, err := ToBase62(id, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(slug) != 11 {
			t.Fatalf("64-bit slug %q len = %d", slug, len(slug))
		}
		got, ok := decode62(slug)
		if !ok || got != id {
			t.Fatalf("64-bit %d encoded to %s decoded to %d ok=%v", id, slug, got, ok)
		}
	}

	seen := map[string]uint64{}
	for id := uint64(0); id < 10000; id++ {
		slug, err := ToBase62(id, 48)
		if err != nil {
			t.Fatal(err)
		}
		if prev, ok := seen[slug]; ok {
			t.Fatalf("slug %s from %d and %d", slug, prev, id)
		}
		seen[slug] = id
	}
}

func decode48(s string) uint64 {
	var n uint64
	for i := 0; i < len(s); i++ {
		n = (n << 6) | uint64(index64(s[i]))
	}
	return n
}

func index64(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'z':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'Z':
		return int(c-'A') + 36
	case c == '-':
		return 62
	case c == '_':
		return 63
	default:
		return -1
	}
}

func decode62(s string) (uint64, bool) {
	var n uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'a' && c <= 'z':
			d = uint64(c-'a') + 10
		case c >= 'A' && c <= 'Z':
			d = uint64(c-'A') + 36
		default:
			return 0, false
		}
		if n > (^uint64(0)-d)/62 {
			return 0, false
		}
		n = n*62 + d
	}
	return n, true
}
