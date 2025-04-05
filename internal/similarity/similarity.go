package similarity

import (
	"math"
	"strings"
	"unicode"
)

// Function represents a similarity function interface
type Function interface {
	// Compare returns a similarity score between 0.0 and 1.0,
	// where 0.0 means completely different and 1.0 means identical
	Compare(a, b string) float64
	// Name returns the name of the similarity function
	Name() string
}

// ExactMatch checks if two strings are exactly equal
type ExactMatch struct{}

func (f ExactMatch) Compare(a, b string) float64 {
	if a == b {
		return 1.0
	}
	return 0.0
}

func (f ExactMatch) Name() string {
	return "ExactMatch"
}

// CaseInsensitiveMatch checks if two strings are equal, ignoring case
type CaseInsensitiveMatch struct{}

func (f CaseInsensitiveMatch) Compare(a, b string) float64 {
	if strings.EqualFold(a, b) {
		return 1.0
	}
	return 0.0
}

func (f CaseInsensitiveMatch) Name() string {
	return "CaseInsensitiveMatch"
}

// JaroWinkler implements the Jaro-Winkler similarity algorithm
// Good for person names and short strings where character order matters
type JaroWinkler struct {
	// Prefix scale factor, default is 0.1
	PrefixScale float64
	// Prefix length to consider, default is 4
	PrefixLength int
}

func NewJaroWinkler() JaroWinkler {
	return JaroWinkler{
		PrefixScale:  0.1,
		PrefixLength: 4,
	}
}

func (f JaroWinkler) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Calculate Jaro similarity
	jaroScore := f.jaro(a, b)

	// Calculate common prefix length
	prefixLen := 0
	maxPrefixLen := min(f.PrefixLength, min(len(a), len(b)))
	for i := 0; i < maxPrefixLen; i++ {
		if a[i] == b[i] {
			prefixLen++
		} else {
			break
		}
	}

	// Apply Winkler adjustment
	return jaroScore + (float64(prefixLen) * f.PrefixScale * (1.0 - jaroScore))
}

func (f JaroWinkler) jaro(a, b string) float64 {
	if a == b {
		return 1.0
	}

	// Ensure a is the shorter string for simplicity
	if len(a) > len(b) {
		a, b = b, a
	}

	// Empty strings have distance 0
	if len(a) == 0 {
		return 0.0
	}

	// Maximum distance to consider characters as matching
	matchDistance := max(len(a), len(b))/2 - 1
	if matchDistance < 0 {
		matchDistance = 0
	}

	// Arrays to track matches
	matchesA := make([]bool, len(a))
	matchesB := make([]bool, len(b))

	// Count matches
	matches := 0
	for i := 0; i < len(a); i++ {
		start := max(0, i-matchDistance)
		end := min(i+matchDistance+1, len(b))

		for j := start; j < end; j++ {
			if !matchesB[j] && a[i] == b[j] {
				matchesA[i] = true
				matchesB[j] = true
				matches++
				break
			}
		}
	}

	// If no matches, strings are completely different
	if matches == 0 {
		return 0.0
	}

	// Count transpositions
	transpositions := 0
	k := 0
	for i := 0; i < len(a); i++ {
		if matchesA[i] {
			for !matchesB[k] {
				k++
			}
			if a[i] != b[k] {
				transpositions++
			}
			k++
		}
	}

	// Calculate Jaro similarity
	m := float64(matches)
	return (m/float64(len(a)) + m/float64(len(b)) + (m-float64(transpositions)/2.0)/m) / 3.0
}

func (f JaroWinkler) Name() string {
	return "JaroWinkler"
}

// Levenshtein calculates similarity using edit distance
// Good for general string comparison where character-level edits matter
type Levenshtein struct{}

func (f Levenshtein) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Calculate Levenshtein distance
	distance := f.distance(a, b)
	maxLen := max(len(a), len(b))

	// Convert distance to similarity score (0-1)
	return 1.0 - float64(distance)/float64(maxLen)
}

func (f Levenshtein) distance(a, b string) int {
	// Convert to runes to handle UTF-8 correctly
	s1 := []rune(a)
	s2 := []rune(b)

	// Create matrix
	rows, cols := len(s1)+1, len(s2)+1
	dist := make([][]int, rows)
	for i := 0; i < rows; i++ {
		dist[i] = make([]int, cols)
		dist[i][0] = i
	}
	for j := 0; j < cols; j++ {
		dist[0][j] = j
	}

	// Fill the matrix
	for i := 1; i < rows; i++ {
		for j := 1; j < cols; j++ {
			var cost int
			if s1[i-1] == s2[j-1] {
				cost = 0
			} else {
				cost = 1
			}
			dist[i][j] = min(
				dist[i-1][j]+1,      // deletion
				dist[i][j-1]+1,      // insertion
				dist[i-1][j-1]+cost, // substitution
			)
		}
	}

	return dist[rows-1][cols-1]
}

func (f Levenshtein) Name() string {
	return "Levenshtein"
}

// Jaccard calculates similarity using sets of tokens (words)
// Good for longer texts where word overlap matters more than exact ordering
type Jaccard struct{}

func (f Jaccard) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Split into tokens (words)
	tokensA := tokenize(a)
	tokensB := tokenize(b)

	// Create sets
	setA := make(map[string]bool)
	setB := make(map[string]bool)
	unionSet := make(map[string]bool)

	for _, token := range tokensA {
		setA[token] = true
		unionSet[token] = true
	}

	for _, token := range tokensB {
		setB[token] = true
		unionSet[token] = true
	}

	// Calculate intersection size
	intersection := 0
	for token := range setA {
		if setB[token] {
			intersection++
		}
	}

	// Calculate Jaccard similarity: |A ∩ B| / |A ∪ B|
	return float64(intersection) / float64(len(unionSet))
}

func (f Jaccard) Name() string {
	return "Jaccard"
}

// Cosine calculates cosine similarity between token sets
// Good for documents where term frequency matters
type Cosine struct{}

func (f Cosine) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Tokenize and count term frequencies
	tokensA := tokenize(a)
	tokensB := tokenize(b)

	vecA := make(map[string]int)
	vecB := make(map[string]int)

	for _, token := range tokensA {
		vecA[token]++
	}

	for _, token := range tokensB {
		vecB[token]++
	}

	// Calculate dot product
	dotProduct := 0.0
	for token, countA := range vecA {
		if countB, exists := vecB[token]; exists {
			dotProduct += float64(countA * countB)
		}
	}

	// Calculate magnitudes
	magA := 0.0
	for _, count := range vecA {
		magA += float64(count * count)
	}
	magA = math.Sqrt(magA)

	magB := 0.0
	for _, count := range vecB {
		magB += float64(count * count)
	}
	magB = math.Sqrt(magB)

	// Guard against division by zero
	if magA == 0 || magB == 0 {
		return 0.0
	}

	return dotProduct / (magA * magB)
}

func (f Cosine) Name() string {
	return "Cosine"
}

// ContainedIn checks if one string is contained in another
type ContainedIn struct {
	// Whether to ignore case
	IgnoreCase bool
}

func (f ContainedIn) Compare(a, b string) float64 {
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	if f.IgnoreCase {
		a = strings.ToLower(a)
		b = strings.ToLower(b)
	}

	if strings.Contains(b, a) || strings.Contains(a, b) {
		// Return a score based on the length ratio of the shorter to the longer string
		minLen := min(len(a), len(b))
		maxLen := max(len(a), len(b))
		return float64(minLen) / float64(maxLen)
	}
	return 0.0
}

func (f ContainedIn) Name() string {
	return "ContainedIn"
}

// Helper function to tokenize a string into words
func tokenize(s string) []string {
	var tokens []string
	inToken := false
	start := 0

	// Process each rune in the string
	for i, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if !inToken {
				inToken = true
				start = i
			}
		} else {
			if inToken {
				inToken = false
				tokens = append(tokens, strings.ToLower(s[start:i]))
			}
		}
	}

	// Handle the last token if it ends at the end of the string
	if inToken {
		tokens = append(tokens, strings.ToLower(s[start:]))
	}

	return tokens
}

// Helper function to find the minimum of 2-3 integers
func min(a, b int, rest ...int) int {
	result := a
	if b < result {
		result = b
	}
	for _, v := range rest {
		if v < result {
			result = v
		}
	}
	return result
}

// Helper function to find the maximum of 2-3 integers
func max(a, b int, rest ...int) int {
	result := a
	if b > result {
		result = b
	}
	for _, v := range rest {
		if v > result {
			result = v
		}
	}
	return result
}
