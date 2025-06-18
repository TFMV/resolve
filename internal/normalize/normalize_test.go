package normalize

import (
	"testing"

	"github.com/TFMV/resolve/internal/config"
)

func newTestNormalizer() *Normalizer {
	cfg := &config.Config{}
	cfg.Normalization.EnableLowercase = true
	cfg.Normalization.EnableStopwords = true
	cfg.Normalization.NameOptions = map[string]bool{"remove_legal_suffixes": true, "normalize_initials": true}
	cfg.Normalization.AddressOptions = map[string]bool{"remove_apartment_numbers": true}
	cfg.Normalization.PhoneOptions = map[string]bool{"e164_format": true}
	cfg.Normalization.EmailOptions = map[string]bool{"lowercase_domain": true}
	return NewNormalizer(cfg)
}

func TestNormalizeText(t *testing.T) {
	n := newTestNormalizer()
	got := n.NormalizeText("  The quick  brown fox  ")
	want := "quick brown fox"
	if got != want {
		t.Errorf("expected %q got %q", want, got)
	}
}

func TestNormalizeName(t *testing.T) {
	n := newTestNormalizer()
	if got := n.NormalizeName("ACME INC."); got != "acme" {
		t.Errorf("NormalizeName remove suffix: %q", got)
	}
	if got := n.NormalizeName("J. D. Salinger"); got != "j d salinger" {
		t.Errorf("NormalizeName initials: %q", got)
	}
}

func TestNormalizeZip(t *testing.T) {
	n := newTestNormalizer()
	if got := n.NormalizeZip("12345-6789"); got != "12345" {
		t.Errorf("expected 12345 got %s", got)
	}
}
