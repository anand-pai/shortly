package loadtest

import (
	"math/rand"
	"net/url"
	"testing"
)

func TestLongURL(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	gen := newURLGen(80)
	got := gen.next(rng)
	if len(got) != 80 {
		t.Fatalf("len %d", len(got))
	}
	parsed, err := url.Parse(got)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		t.Fatalf("url %q err %v", got, err)
	}
	again := rand.New(rand.NewSource(5))
	if newURLGen(80).next(again) != got {
		t.Fatal("same seed produced a different URL")
	}
}

func TestCompanyIDs(t *testing.T) {
	ids := companyIDs(4)
	if len(ids) != 4 || ids[0] != "c-0" || ids[3] != "c-3" {
		t.Fatalf("%v", ids)
	}
}

func TestSlugFromShortURL(t *testing.T) {
	slug, err := slugFromShortURL("https://shortly-svc/Ab12Cd34")
	if err != nil || slug != "Ab12Cd34" {
		t.Fatalf("slug %q err %v", slug, err)
	}
	slug, err = slugFromShortURL("https://shortly-svc/Ab12Cd34?x=1")
	if err != nil || slug != "Ab12Cd34" {
		t.Fatalf("query slug %q err %v", slug, err)
	}
	if _, err := slugFromShortURL("https://shortly-svc/short"); err == nil {
		t.Fatal("expected reject")
	}
	if _, err := slugFromShortURL("https://shortly-svc/ab/cd"); err == nil {
		t.Fatal("expected reject")
	}
}
