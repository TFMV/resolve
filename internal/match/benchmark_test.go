package match

import (
	"context"
	"strconv"
	"testing"

	"github.com/TFMV/resolve/internal/cluster"
	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/normalize"
)

func benchmarkBatch(b *testing.B, records int) {
	cfg := &config.Config{}
	n := normalize.NewNormalizer(cfg)
	emb := embed.NewMockEmbeddingService(16)
	cs := cluster.NewService(&cluster.Config{Enabled: true, Fields: []string{"name", "zip"}}, n)
	ctx := context.Background()

	fields := map[string]string{
		"name":    "Acme Corp",
		"address": "123 Main St",
		"zip":     "12345",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < records; j++ {
			data := make(map[string]string)
			for k, v := range fields {
				data[k] = v + strconv.Itoa(j)
			}
			norm := n.NormalizeEntity(data)
			text := combineFields(norm)
			emb.GetEmbedding(ctx, text)
			cs.GenerateClusterKey(ctx, norm)
		}
	}
}

func BenchmarkBatchResolution100(b *testing.B) { benchmarkBatch(b, 100) }
func BenchmarkBatchResolution1k(b *testing.B)  { benchmarkBatch(b, 1000) }
func BenchmarkBatchResolution10k(b *testing.B) { benchmarkBatch(b, 10000) }
