package similarity

import "testing"

func TestPhoneSimilarity(t *testing.T) {
	f := NewPhoneSimilarity()
	tests := []struct {
		a, b string
		min  float64
	}{
		{"123-456-7890", "(123)456-7890", 1.0},
		{"1234567", "123-4567", 0.9},
		{"555-1234", "999-8888", 0.0},
	}
	for _, tt := range tests {
		if score := f.Compare(tt.a, tt.b); score < tt.min {
			t.Errorf("%s vs %s expected >= %.2f got %.2f", tt.a, tt.b, tt.min, score)
		}
	}
}
