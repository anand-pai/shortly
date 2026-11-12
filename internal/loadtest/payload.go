package loadtest

import (
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"path"
	"regexp"
	"strconv"
)

const longURLPrefix = "https://lt.example/"
const urlAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

var (
	errBadSlug  = errors.New("short url did not contain an 8 character slug")
	slugPattern = regexp.MustCompile(`^[0-9A-Za-z]{8}$`)
)

func companyIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "c-" + strconv.Itoa(i)
	}
	return ids
}

type urlGen struct {
	n   int
	buf []byte
}

func newURLGen(n int) *urlGen {
	if n < len(longURLPrefix)+1 {
		n = len(longURLPrefix) + 1
	}
	return &urlGen{n: n}
}

func (g *urlGen) next(rng *rand.Rand) string {
	if cap(g.buf) < g.n {
		g.buf = make([]byte, g.n)
	}
	g.buf = g.buf[:g.n]
	copy(g.buf, longURLPrefix)
	for i := len(longURLPrefix); i < g.n; i++ {
		g.buf[i] = urlAlphabet[rng.Intn(len(urlAlphabet))]
	}
	return string(g.buf)
}

func slugFromShortURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %s", errBadSlug, err.Error())
	}
	slug := path.Base(parsed.Path)
	if !slugPattern.MatchString(slug) {
		return "", fmt.Errorf("%w: %q", errBadSlug, raw)
	}
	return slug, nil
}
