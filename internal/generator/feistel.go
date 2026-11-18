package generator

import "fmt"

const (
	Max48Bits      = (uint64(1) << 48) - 1
	Mask24         = (uint64(1) << 24) - 1
	Base62Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	// Alphabet64 is base62 plus two URL-safe symbols. 64^8 = 2^48, so eight
	// characters hold every 48-bit id exactly once. Base62 cannot: 62^8 < 2^48.
	Alphabet64 = Base62Alphabet + "-_"
)

// Round keys for 24-bit half-mixing
var roundKeys = [4]uint64{
	0xA35B21,
	0xF4E19C,
	0x5C8A73,
	0x91D0E4,
}

// FeistelEncrypt ensures a strict 1-to-1 bijective mapping: Input != Input2 => Output != Output2
func FeistelEncrypt(input uint64) uint64 {
	input = input & Max48Bits

	left := (input >> 24) & Mask24
	right := input & Mask24

	for i := 0; i < 4; i++ {
		nextLeft := right

		// Non-linear round function F(right, key)
		x := (right ^ roundKeys[i]) & Mask24
		x = (x ^ (x >> 13)) * 0x5deece66d
		fOut := (x ^ (x >> 16)) & Mask24

		nextRight := (left ^ fOut) & Mask24
		left = nextLeft
		right = nextRight
	}

	return (left << 24) | right
}

// ToBase62 encodes an id in a fixed number of steps, with no retries.
// A 48-bit id becomes 8 characters. Each character carries 6 bits, and 8×6 = 48,
// so the mapping is one-to-one. A 64-bit id becomes 11 base62 characters;
// 62^11 is larger than 2^64, so those ids do not collide either.
func ToBase62(val uint64, bits int) (string, error) {
	switch bits {
	case 48:
		if val > Max48Bits {
			return "", fmt.Errorf("value %d does not fit in 48 bits", val)
		}
		buf := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			buf[i] = Alphabet64[val&63]
			val >>= 6
		}
		return string(buf), nil
	case 64:
		return EncodeBase62(val, 11), nil
	default:
		return "", fmt.Errorf("unsupported id width %d", bits)
	}
}

// EncodeBase62 writes val as a fixed-width base62 string.
func EncodeBase62(val uint64, width int) string {
	buf := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		buf[i] = Base62Alphabet[val%62]
		val /= 62
	}
	return string(buf)
}

// mixBits is a balanced Feistel permutation over an even bit width (1..64).
// An input below 2^bits maps to a different output in the same range.
func mixBits(input uint64, bits int) uint64 {
	half := uint(bits / 2)
	mask := uint64(1)<<half - 1
	if bits < 64 {
		input &= uint64(1)<<uint(bits) - 1
	}

	left := (input >> half) & mask
	right := input & mask
	for i := 0; i < 4; i++ {
		nextLeft := right
		x := (right ^ (roundKeys[i] & mask)) & mask
		x = (x ^ (x >> 13)) * 0x5deece66d
		fOut := (x ^ (x >> 16)) & mask
		nextRight := (left ^ fOut) & mask
		left = nextLeft
		right = nextRight
	}
	return (left << half) | right
}
