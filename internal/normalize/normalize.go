package normalize

import (
	"regexp"
	"strings"
)

var (
	// Common patterns for standardization
	nonAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9\s]`)
	multipleSpaces  = regexp.MustCompile(`\s+`)

	// Common abbreviations to standardize
	abbreviations = map[string]string{
		"inc":   "incorporated",
		"llc":   "limited liability company",
		"ltd":   "limited",
		"corp":  "corporation",
		"co":    "company",
		"intl":  "international",
		"&":     "and",
		"tech":  "technology",
		"svcs":  "services",
		"svc":   "service",
		"mfg":   "manufacturing",
		"hldgs": "holdings",
		"grp":   "group",
		"assn":  "association",
		"assoc": "associates",
	}
)

// Result represents the result of normalization, including both the normalized text
// and information about what changes were made
type Result struct {
	Original              string `json:"original"`
	Normalized            string `json:"normalized"`
	LowerCased            bool   `json:"lower_cased"`
	PunctuationFixed      bool   `json:"punctuation_fixed"`
	AbbreviationsExpanded bool   `json:"abbreviations_expanded"`
}

// Normalize standardizes an input string for better matching
// 1. Convert to lowercase
// 2. Remove punctuation
// 3. Standardize common abbreviations
// 4. Normalize whitespace
func Normalize(input string) string {
	if input == "" {
		return ""
	}

	// Convert to lowercase
	normalized := strings.ToLower(input)

	// Remove punctuation
	normalized = nonAlphaNumeric.ReplaceAllString(normalized, " ")

	// Standardize abbreviations
	words := strings.Fields(normalized)
	for i, word := range words {
		if expanded, exists := abbreviations[word]; exists {
			words[i] = expanded
		}
	}

	// Join and normalize whitespace
	normalized = strings.Join(words, " ")
	normalized = multipleSpaces.ReplaceAllString(normalized, " ")

	return strings.TrimSpace(normalized)
}

// NormalizeWithDetails returns both the normalized string and information about what was changed
func NormalizeWithDetails(input string) Result {
	if input == "" {
		return Result{
			Original:   "",
			Normalized: "",
		}
	}

	result := Result{
		Original: input,
	}

	// Convert to lowercase
	lowerCased := strings.ToLower(input)
	result.LowerCased = lowerCased != input

	// Remove punctuation
	withoutPunctuation := nonAlphaNumeric.ReplaceAllString(lowerCased, " ")
	result.PunctuationFixed = withoutPunctuation != lowerCased

	// Standardize abbreviations
	words := strings.Fields(withoutPunctuation)
	abbreviationsExpanded := false
	for i, word := range words {
		if expanded, exists := abbreviations[word]; exists {
			words[i] = expanded
			abbreviationsExpanded = true
		}
	}
	result.AbbreviationsExpanded = abbreviationsExpanded

	// Join and normalize whitespace
	normalized := strings.Join(words, " ")
	normalized = multipleSpaces.ReplaceAllString(normalized, " ")
	result.Normalized = strings.TrimSpace(normalized)

	return result
}

// StandardizeCommonAbbreviations replaces common abbreviations with their full forms
func StandardizeCommonAbbreviations(input string) string {
	words := strings.Fields(strings.ToLower(input))
	for i, word := range words {
		if expanded, exists := abbreviations[word]; exists {
			words[i] = expanded
		}
	}
	return strings.Join(words, " ")
}

// AddAbbreviation adds a custom abbreviation to the standardization map
func AddAbbreviation(abbreviation, expansion string) {
	abbreviations[strings.ToLower(abbreviation)] = strings.ToLower(expansion)
}
