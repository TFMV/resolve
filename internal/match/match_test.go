package match

import "testing"

func TestParseQueryFields(t *testing.T) {
	tests := []struct {
		input string
		want  map[string]string
	}{
		{"name=Acme", map[string]string{"name": "Acme"}},
		{"name=Acme;city=NY", map[string]string{"name": "Acme", "city": "NY"}},
		{"name=Acme,address=123 St", map[string]string{"name": "Acme", "address": "123 St"}},
		{"noequals", map[string]string{}},
		{"a=1;b=2;c=3", map[string]string{"a": "1", "b": "2", "c": "3"}},
	}
	for _, tt := range tests {
		got := parseQueryFields(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("%q: expected %d fields got %d", tt.input, len(tt.want), len(got))
			continue
		}
		for k, v := range tt.want {
			if got[k] != v {
				t.Errorf("%q: expected %s=%s got %s", tt.input, k, v, got[k])
			}
		}
	}
}

func TestComputeWeightedScore(t *testing.T) {
	scores := map[string]FieldScore{
		"name":  {Score: 0.8},
		"phone": {Score: 0.5},
	}
	weights := map[string]float32{
		"name":  0.6,
		"phone": 0.4,
	}
	got := computeWeightedScore(scores, weights)
	want := float32(0.8*0.6 + 0.5*0.4)
	if got != want {
		t.Errorf("expected %f got %f", want, got)
	}
}
