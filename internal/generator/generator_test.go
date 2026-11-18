package generator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type memStore struct {
	mu    sync.Mutex
	start map[uint64]uint64
}

func newMemStore(start map[uint64]uint64) *memStore {
	copied := make(map[uint64]uint64, len(start))
	for channel, seq := range start {
		copied[channel] = seq
	}
	return &memStore{start: copied}
}

func (m *memStore) LoadInitialSequence(_ context.Context, _, _, channel uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.start[channel], nil
}

func (m *memStore) SaveCheckpoint(context.Context, uint64, uint64, uint64, uint64) error {
	return nil
}

func TestChannelLayoutsHaveSeparateWidths(t *testing.T) {
	sms := channelLayouts[ChannelSMS]
	rcs := channelLayouts[ChannelRCSWA]

	if sms.seqBits != 36 || sms.totalBits != 48 || sms.slugLen != 8 {
		t.Fatalf("sms layout = %+v, want 36 counter bits, 48 total bits, 8 char slug", sms)
	}
	if rcs.seqBits != 52 || rcs.totalBits != 64 || rcs.slugLen != 11 {
		t.Fatalf("rcs layout = %+v, want 52 counter bits, 64 total bits, 11 char slug", rcs)
	}
}

func TestGenerateSlugUsesTheChannelCounter(t *testing.T) {
	t.Setenv("REGION_ID", "0")
	t.Setenv("POD_NAME", "")

	gen, err := NewSlugGenerator(context.Background(), newMemStore(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gen.Close)

	sms1, err := gen.GenerateSlug(ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	rcs1, err := gen.GenerateSlug(ChannelRCSWA)
	if err != nil {
		t.Fatal(err)
	}
	sms2, err := gen.GenerateSlug(ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}

	smsLayout := channelLayouts[ChannelSMS]
	rcsLayout := channelLayouts[ChannelRCSWA]
	wantSMS1, err := ToBase62(mixBits(1, smsLayout.totalBits), smsLayout.totalBits)
	if err != nil {
		t.Fatal(err)
	}
	wantSMS2, err := ToBase62(mixBits(2, smsLayout.totalBits), smsLayout.totalBits)
	if err != nil {
		t.Fatal(err)
	}
	wantRCS1, err := ToBase62(mixBits(1, rcsLayout.totalBits), rcsLayout.totalBits)
	if err != nil {
		t.Fatal(err)
	}
	if sms1 != wantSMS1 || sms2 != wantSMS2 || rcs1 != wantRCS1 {
		t.Fatalf("slugs sms=%s,%s rcs=%s, want %s,%s and %s", sms1, sms2, rcs1, wantSMS1, wantSMS2, wantRCS1)
	}
	if len(sms1) != 8 || len(rcs1) != 11 {
		t.Fatalf("lengths sms=%d rcs=%d", len(sms1), len(rcs1))
	}
	if strings.TrimLeft(sms1, "0") == strings.TrimLeft(rcs1, "0") {
		t.Fatal("channels produced the same slug body")
	}
}

func TestGenerateSlugResumesEachChannel(t *testing.T) {
	t.Setenv("REGION_ID", "1")
	t.Setenv("POD_NAME", "shortener-app-3")

	gen, err := NewSlugGenerator(context.Background(), newMemStore(map[uint64]uint64{
		ChannelSMS:   10,
		ChannelRCSWA: 40,
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gen.Close)

	identity, err := GetWorkerIdentity()
	if err != nil {
		t.Fatal(err)
	}

	sms, err := gen.GenerateSlug(ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	rcs, err := gen.GenerateSlug(ChannelRCSWA)
	if err != nil {
		t.Fatal(err)
	}

	smsLayout := channelLayouts[ChannelSMS]
	rcsLayout := channelLayouts[ChannelRCSWA]
	smsPrefix := (identity.RegionID << (WorkerBits + smsLayout.seqBits)) | (identity.WorkerID << smsLayout.seqBits)
	rcsPrefix := (identity.RegionID << (WorkerBits + rcsLayout.seqBits)) | (identity.WorkerID << rcsLayout.seqBits)
	wantSMS, err := ToBase62(mixBits(smsPrefix|11, smsLayout.totalBits), smsLayout.totalBits)
	if err != nil {
		t.Fatal(err)
	}
	wantRCS, err := ToBase62(mixBits(rcsPrefix|41, rcsLayout.totalBits), rcsLayout.totalBits)
	if err != nil {
		t.Fatal(err)
	}
	if sms != wantSMS || rcs != wantRCS {
		t.Fatalf("slugs = %s %s, want %s %s", sms, rcs, wantSMS, wantRCS)
	}
}

func TestGenerateSlugRejectsUnknownChannel(t *testing.T) {
	t.Setenv("REGION_ID", "0")
	t.Setenv("POD_NAME", "")

	gen, err := NewSlugGenerator(context.Background(), newMemStore(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gen.Close)

	if _, err = gen.GenerateSlug(2); !errors.Is(err, ErrInvalidChannel) {
		t.Fatalf("err = %v, want ErrInvalidChannel", err)
	}
	sms, err := gen.GenerateSlug(ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	want, err := ToBase62(mixBits(1, channelLayouts[ChannelSMS].totalBits), channelLayouts[ChannelSMS].totalBits)
	if err != nil {
		t.Fatal(err)
	}
	if sms != want {
		t.Fatal("rejected channel advanced the SMS counter")
	}
}

func TestSMSCounterExhaustsAt36Bits(t *testing.T) {
	t.Setenv("REGION_ID", "0")
	t.Setenv("POD_NAME", "")

	gen, err := NewSlugGenerator(context.Background(), newMemStore(map[uint64]uint64{
		ChannelSMS: channelLayouts[ChannelSMS].maxSeq,
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gen.Close)

	if _, err = gen.GenerateSlug(ChannelSMS); err == nil {
		t.Fatal("expected the 36-bit SMS counter to be exhausted")
	}
	rcs, err := gen.GenerateSlug(ChannelRCSWA)
	if err != nil {
		t.Fatal(err)
	}
	if len(rcs) != channelLayouts[ChannelRCSWA].slugLen {
		t.Fatalf("rcs slug len = %d", len(rcs))
	}
}

func TestPodIDIsTenBits(t *testing.T) {
	t.Setenv("REGION_ID", "0")

	t.Setenv("POD_NAME", "shortener-app-1023")
	if _, err := GetWorkerIdentity(); err != nil {
		t.Fatalf("pod 1023 rejected: %v", err)
	}

	t.Setenv("POD_NAME", "shortener-app-1024")
	if _, err := GetWorkerIdentity(); err == nil {
		t.Fatal("pod 1024 accepted, want 10-bit limit")
	}
}

func TestMixBitsIsInjectiveWithinWidth(t *testing.T) {
	for _, bits := range []int{channelLayouts[ChannelSMS].totalBits, channelLayouts[ChannelRCSWA].totalBits} {
		seen := make(map[uint64]struct{}, 4096)
		for i := uint64(0); i < 4096; i++ {
			out := mixBits(i, bits)
			if bits < 64 && out >= uint64(1)<<uint(bits) {
				t.Fatalf("bits=%d: output %d out of range", bits, out)
			}
			if _, dup := seen[out]; dup {
				t.Fatalf("bits=%d: collision at input %d", bits, i)
			}
			seen[out] = struct{}{}
		}
	}
}
