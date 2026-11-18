package generator

import (
	"context"
	"errors"
	"fmt"
)

const (
	// ChannelSMS is CreateUrlRequest.channel 0.
	ChannelSMS = 0
	// ChannelRCSWA is CreateUrlRequest.channel 1 (RCS and WhatsApp).
	ChannelRCSWA = 1
)

// Each channel has its own counter width. The id is
// [ region 2 | pod 10 | counter ], so the total width is counter + 12.
//
//	SMS    total 48 bits: [ region 2 | pod 10 | counter 36 ]
//	RCS/WA total 64 bits: [ region 2 | pod 10 | counter 52 ]
const (
	smsCounterBits   = 36
	rcsWaCounterBits = 52
)

var ErrInvalidChannel = errors.New("channel must be 0 (SMS) or 1 (RCS/WA)")

type idLayout struct {
	seqBits   uint
	totalBits int
	slugLen   int
	maxSeq    uint64
}

func layoutFor(counterBits uint) idLayout {
	totalBits := int(counterBits) + RegionBits + WorkerBits
	return idLayout{
		seqBits:   counterBits,
		totalBits: totalBits,
		slugLen:   encodedLen(totalBits),
		maxSeq:    uint64(1)<<counterBits - 1,
	}
}

func encodedLen(bits int) int {
	switch bits {
	case 48:
		return 8
	case 64:
		return 11
	default:
		return 0
	}
}

var channelLayouts = [ChannelRCSWA + 1]idLayout{
	ChannelSMS:   layoutFor(smsCounterBits),
	ChannelRCSWA: layoutFor(rcsWaCounterBits),
}

type SlugGenerator struct {
	prefixes [ChannelRCSWA + 1]uint64
	counters [ChannelRCSWA + 1]*InMemoryCounter
}

func NewSlugGenerator(ctx context.Context, store StateStore) (*SlugGenerator, error) {
	identity, err := GetWorkerIdentity()
	if err != nil {
		return nil, err
	}

	gen := &SlugGenerator{}
	for channel := 0; channel < len(gen.counters); channel++ {
		layout := channelLayouts[channel]
		startSeq, err := store.LoadInitialSequence(ctx, identity.RegionID, identity.WorkerID, uint64(channel))
		if err != nil {
			gen.Close()
			return nil, err
		}
		gen.prefixes[channel] = (identity.RegionID << (WorkerBits + layout.seqBits)) | (identity.WorkerID << layout.seqBits)
		gen.counters[channel] = NewInMemoryCounter(
			identity.RegionID,
			identity.WorkerID,
			uint64(channel),
			startSeq,
			layout.maxSeq,
			layout.seqBits,
			store,
		)
	}

	return gen, nil
}

// GenerateSlug increments only the counter for channel and encodes that counter's own bit width.
// Channel 0 is SMS (48-bit id, 8-character slug). Channel 1 is RCS/WA (64-bit id, 11-character slug).
func (g *SlugGenerator) GenerateSlug(channel int) (string, error) {
	if channel < ChannelSMS || channel > ChannelRCSWA {
		return "", fmt.Errorf("%w: %d", ErrInvalidChannel, channel)
	}

	seq, err := g.counters[channel].Next()
	if err != nil {
		return "", err
	}

	layout := channelLayouts[channel]
	raw := g.prefixes[channel] | seq
	return ToBase62(mixBits(raw, layout.totalBits), layout.totalBits)
}

func (g *SlugGenerator) Close() {
	for _, counter := range g.counters {
		if counter != nil {
			counter.Close()
		}
	}
}
