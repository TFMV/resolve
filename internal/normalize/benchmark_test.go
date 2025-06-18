package normalize

import (
	"strconv"
	"testing"

	"github.com/TFMV/resolve/internal/config"
)

func BenchmarkNormalizeEntity(b *testing.B) {
	cfg := &config.Config{}
	cfg.Normalization.EnableLowercase = true
	cfg.Normalization.EnableStopwords = true
	cfg.Normalization.NameOptions = map[string]bool{"remove_legal_suffixes": true, "normalize_initials": true}
	cfg.Normalization.AddressOptions = map[string]bool{"remove_apartment_numbers": true}
	n := NewNormalizer(cfg)

	base := map[string]string{"name": "Test Corp", "address": "123 Main St Apt 4", "zip": "12345-6789"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields := make(map[string]string, len(base))
		for k, v := range base {
			fields[k] = v + strconv.Itoa(i)
		}
		n.NormalizeEntity(fields)
	}
}
