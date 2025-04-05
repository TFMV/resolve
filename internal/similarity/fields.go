package similarity

import (
	"regexp"
	"strings"
)

// NameSimilarity is specialized for comparing person or business names
type NameSimilarity struct {
	// Internal algorithms
	jaroWinkler     JaroWinkler
	tokenJaccard    Jaccard
	containedIn     ContainedIn
	exactMatch      ExactMatch
	caseInsensitive CaseInsensitiveMatch

	// Legal suffix removal regex
	legalSuffixRegex *regexp.Regexp
}

// NewNameSimilarity creates a new name similarity function
func NewNameSimilarity() *NameSimilarity {
	return &NameSimilarity{
		jaroWinkler:      NewJaroWinkler(),
		tokenJaccard:     Jaccard{},
		containedIn:      ContainedIn{IgnoreCase: true},
		exactMatch:       ExactMatch{},
		caseInsensitive:  CaseInsensitiveMatch{},
		legalSuffixRegex: regexp.MustCompile(`(?i)\s+(inc\.?|incorporated|corp\.?|corporation|llc|ltd\.?|limited|llp|l\.l\.p\.?|pllc|p\.l\.l\.c\.?|pc|p\.c\.?)$`),
	}
}

// Compare calculates similarity between two names using a combination of metrics
func (f *NameSimilarity) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Check for exact match first
	if f.exactMatch.Compare(a, b) == 1.0 {
		return 1.0
	}

	// Preprocess names
	a = f.preprocess(a)
	b = f.preprocess(b)

	// Check for exact match after preprocessing
	if f.caseInsensitive.Compare(a, b) == 1.0 {
		return 1.0
	}

	// Compute various similarity scores
	jaroScore := f.jaroWinkler.Compare(a, b)
	tokenScore := f.tokenJaccard.Compare(a, b)
	containmentScore := f.containedIn.Compare(a, b)

	// Combine scores with appropriate weighting
	// Give more weight to Jaro-Winkler for names as it's particularly good for names
	combinedScore := (jaroScore * 0.6) + (tokenScore * 0.3) + (containmentScore * 0.1)

	return combinedScore
}

// Preprocess normalizes names for better comparison
func (f *NameSimilarity) preprocess(name string) string {
	// Convert to lowercase
	name = strings.ToLower(name)

	// Remove legal suffixes
	name = f.legalSuffixRegex.ReplaceAllString(name, "")

	// Remove extra spaces
	name = strings.TrimSpace(name)
	spaceRegex := regexp.MustCompile(`\s+`)
	name = spaceRegex.ReplaceAllString(name, " ")

	return name
}

func (f *NameSimilarity) Name() string {
	return "NameSimilarity"
}

// AddressSimilarity is specialized for comparing address strings
type AddressSimilarity struct {
	// Internal algorithms
	tokenJaccard    Jaccard
	jaroWinkler     JaroWinkler
	containedIn     ContainedIn
	exactMatch      ExactMatch
	caseInsensitive CaseInsensitiveMatch

	// Address normalization regexes
	numericRegex     *regexp.Regexp
	directionalRegex *regexp.Regexp
	streetTypeRegex  *regexp.Regexp
	unitRegex        *regexp.Regexp

	// Mappings for normalization
	streetTypes map[string]string
	directions  map[string]string
}

// NewAddressSimilarity creates a new address similarity function
func NewAddressSimilarity() *AddressSimilarity {
	return &AddressSimilarity{
		tokenJaccard:     Jaccard{},
		jaroWinkler:      NewJaroWinkler(),
		containedIn:      ContainedIn{IgnoreCase: true},
		exactMatch:       ExactMatch{},
		caseInsensitive:  CaseInsensitiveMatch{},
		numericRegex:     regexp.MustCompile(`\d+`),
		directionalRegex: regexp.MustCompile(`(?i)\b(north|south|east|west|n\.?|s\.?|e\.?|w\.?|ne|nw|se|sw)\b`),
		streetTypeRegex:  regexp.MustCompile(`(?i)\b(street|st\.?|avenue|ave\.?|boulevard|blvd\.?|road|rd\.?|drive|dr\.?|lane|ln\.?|court|ct\.?|circle|cir\.?|place|pl\.?|way|parkway|pkwy\.?|highway|hwy\.?|expressway|expy\.?)\b`),
		unitRegex:        regexp.MustCompile(`(?i)(\s+)(apt|apartment|ste|suite|unit|#)\.?\s+[a-z0-9-]+`),
		streetTypes: map[string]string{
			"street":    "st",
			"st":        "st",
			"avenue":    "ave",
			"ave":       "ave",
			"boulevard": "blvd",
			"blvd":      "blvd",
			"road":      "rd",
			"rd":        "rd",
			"drive":     "dr",
			"dr":        "dr",
			"lane":      "ln",
			"ln":        "ln",
			"court":     "ct",
			"ct":        "ct",
			"circle":    "cir",
			"cir":       "cir",
			"place":     "pl",
			"pl":        "pl",
			"way":       "way",
			"parkway":   "pkwy",
			"pkwy":      "pkwy",
			"highway":   "hwy",
			"hwy":       "hwy",
		},
		directions: map[string]string{
			"north": "n",
			"n":     "n",
			"south": "s",
			"s":     "s",
			"east":  "e",
			"e":     "e",
			"west":  "w",
			"w":     "w",
			"ne":    "ne",
			"nw":    "nw",
			"se":    "se",
			"sw":    "sw",
		},
	}
}

// Compare calculates similarity between two addresses
func (f *AddressSimilarity) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Check for exact match first
	if f.exactMatch.Compare(a, b) == 1.0 {
		return 1.0
	}

	// Preprocess addresses
	a = f.preprocess(a)
	b = f.preprocess(b)

	// Check for exact match after preprocessing
	if f.caseInsensitive.Compare(a, b) == 1.0 {
		return 1.0
	}

	// Extract numeric components (often the house/building number)
	aNumbers := f.numericRegex.FindAllString(a, -1)
	bNumbers := f.numericRegex.FindAllString(b, -1)

	// If we have house numbers and they don't match, reduce the similarity
	numberMatch := 1.0
	if len(aNumbers) > 0 && len(bNumbers) > 0 {
		if aNumbers[0] != bNumbers[0] {
			numberMatch = 0.3 // Strong penalty for different house numbers
		}
	}

	// Calculate token-based similarity (works well for addresses)
	tokenScore := f.tokenJaccard.Compare(a, b)

	// Calculate string-based similarity
	jaroScore := f.jaroWinkler.Compare(a, b)

	// Calculate containment (handles abbreviations and partial matches)
	containmentScore := f.containedIn.Compare(a, b)

	// Combine scores with weights appropriate for addresses
	// Token-based similarity is more important for addresses
	combinedScore := (tokenScore * 0.5) + (jaroScore * 0.2) + (containmentScore * 0.3)

	// Apply house number penalty
	return combinedScore * numberMatch
}

// Preprocess normalizes addresses for better comparison
func (f *AddressSimilarity) preprocess(address string) string {
	// Convert to lowercase
	address = strings.ToLower(address)

	// Remove apartment/unit numbers
	address = f.unitRegex.ReplaceAllString(address, "")

	// Standardize street types
	address = f.streetTypeRegex.ReplaceAllStringFunc(address, func(match string) string {
		match = strings.ToLower(match)
		for fullType, abbr := range f.streetTypes {
			if strings.Contains(match, fullType) {
				return abbr
			}
		}
		return match
	})

	// Standardize directionals
	address = f.directionalRegex.ReplaceAllStringFunc(address, func(match string) string {
		match = strings.ToLower(match)
		for full, abbr := range f.directions {
			if strings.Contains(match, full) {
				return abbr
			}
		}
		return match
	})

	// Remove extra spaces
	address = strings.TrimSpace(address)
	spaceRegex := regexp.MustCompile(`\s+`)
	address = spaceRegex.ReplaceAllString(address, " ")

	return address
}

func (f *AddressSimilarity) Name() string {
	return "AddressSimilarity"
}

// PhoneSimilarity is specialized for comparing phone numbers
type PhoneSimilarity struct {
	// Internal algorithms
	exactMatch ExactMatch

	// Phone normalization regex
	digitRegex *regexp.Regexp
}

// NewPhoneSimilarity creates a new phone similarity function
func NewPhoneSimilarity() *PhoneSimilarity {
	return &PhoneSimilarity{
		exactMatch: ExactMatch{},
		digitRegex: regexp.MustCompile(`\d`),
	}
}

// Compare calculates similarity between two phone numbers
func (f *PhoneSimilarity) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Extract only digits
	aDigits := strings.Join(f.digitRegex.FindAllString(a, -1), "")
	bDigits := strings.Join(f.digitRegex.FindAllString(b, -1), "")

	// Handle empty result after digit extraction
	if aDigits == "" && bDigits == "" {
		return 1.0
	}
	if aDigits == "" || bDigits == "" {
		return 0.0
	}

	// For phone numbers, we're primarily interested in exact match after normalization
	if aDigits == bDigits {
		return 1.0
	}

	// For partial matches, consider the last N digits (usually most important)
	// Get the last digits (try up to 10, but handle shorter numbers)
	aLastDigits := getLastN(aDigits, 10)
	bLastDigits := getLastN(bDigits, 10)

	// Count matching digits from the end
	matchingDigits := 0
	for i := 1; i <= min(len(aLastDigits), len(bLastDigits)); i++ {
		if aLastDigits[len(aLastDigits)-i] == bLastDigits[len(bLastDigits)-i] {
			matchingDigits++
		} else {
			break // Stop at first mismatch
		}
	}

	// Weight by position (last 4 digits most important, then area code, etc.)
	// Different countries have different phone number formats, so this is a simplification
	if matchingDigits >= 10 {
		return 1.0 // Perfect match (all digits match)
	} else if matchingDigits >= 7 {
		return 0.9 // Last 7 digits match (likely same number, different area code or country code)
	} else if matchingDigits >= 4 {
		return 0.7 // Last 4 digits match (could be coincidence, but worth considering)
	} else {
		// For fewer matching digits, return a proportional score
		return float64(matchingDigits) / 10.0
	}
}

func (f *PhoneSimilarity) Name() string {
	return "PhoneSimilarity"
}

// EmailSimilarity is specialized for comparing email addresses
type EmailSimilarity struct {
	// Internal algorithms
	exactMatch      ExactMatch
	caseInsensitive CaseInsensitiveMatch
	jaroWinkler     JaroWinkler

	// Email parts regex
	emailPartsRegex *regexp.Regexp
}

// NewEmailSimilarity creates a new email similarity function
func NewEmailSimilarity() *EmailSimilarity {
	return &EmailSimilarity{
		exactMatch:      ExactMatch{},
		caseInsensitive: CaseInsensitiveMatch{},
		jaroWinkler:     NewJaroWinkler(),
		emailPartsRegex: regexp.MustCompile(`^([^@]+)@(.+)$`),
	}
}

// Compare calculates similarity between two email addresses
func (f *EmailSimilarity) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Check for exact match
	if f.exactMatch.Compare(a, b) == 1.0 {
		return 1.0
	}

	// Check for case-insensitive match
	if f.caseInsensitive.Compare(a, b) == 1.0 {
		return 0.99 // Very high score, but not perfect
	}

	// Parse email parts
	aMatch := f.emailPartsRegex.FindStringSubmatch(a)
	bMatch := f.emailPartsRegex.FindStringSubmatch(b)

	// If either isn't a valid email, use string similarity
	if aMatch == nil || bMatch == nil {
		return f.jaroWinkler.Compare(a, b)
	}

	// Extract username and domain
	aUser, aDomain := aMatch[1], aMatch[2]
	bUser, bDomain := bMatch[1], bMatch[2]

	// Domain match is more important than username match for emails
	domainScore := f.caseInsensitive.Compare(aDomain, bDomain)

	// If domains don't match, emails are likely unrelated
	if domainScore < 1.0 {
		return domainScore * 0.3 // Strong penalty for different domains
	}

	// Calculate username similarity
	userScore := f.jaroWinkler.Compare(aUser, bUser)

	// For emails, domains matching is more important than usernames
	return (userScore * 0.4) + (domainScore * 0.6)
}

func (f *EmailSimilarity) Name() string {
	return "EmailSimilarity"
}

// ZipCodeSimilarity is specialized for comparing postal/zip codes
type ZipCodeSimilarity struct {
	// Internal algorithms
	exactMatch ExactMatch

	// Digit extraction regex
	digitRegex *regexp.Regexp
}

// NewZipCodeSimilarity creates a new zip code similarity function
func NewZipCodeSimilarity() *ZipCodeSimilarity {
	return &ZipCodeSimilarity{
		exactMatch: ExactMatch{},
		digitRegex: regexp.MustCompile(`\d`),
	}
}

// Compare calculates similarity between two zip codes
func (f *ZipCodeSimilarity) Compare(a, b string) float64 {
	// Handle empty strings
	if a == "" && b == "" {
		return 1.0
	}
	if a == "" || b == "" {
		return 0.0
	}

	// Extract only digits
	aDigits := strings.Join(f.digitRegex.FindAllString(a, -1), "")
	bDigits := strings.Join(f.digitRegex.FindAllString(b, -1), "")

	// Handle empty result after digit extraction
	if aDigits == "" && bDigits == "" {
		return 1.0
	}
	if aDigits == "" || bDigits == "" {
		return 0.0
	}

	// Exact match is ideal
	if aDigits == bDigits {
		return 1.0
	}

	// For zip codes, check if the prefix matches (often indicates same area)
	// Zip codes vary by country, but often the first N digits indicate region
	lenA, lenB := len(aDigits), len(bDigits)

	// Determine how many prefix digits to compare
	prefixLen := min(5, min(lenA, lenB))

	// Count matching prefix digits
	matchingDigits := 0
	for i := 0; i < prefixLen; i++ {
		if aDigits[i] == bDigits[i] {
			matchingDigits++
		} else {
			break // Stop at first mismatch
		}
	}

	// Weight by position (first digits most important)
	if matchingDigits == 0 {
		return 0.0 // No match
	} else if matchingDigits >= 5 {
		return 0.95 // Very close match (5+ digits)
	} else if matchingDigits >= 3 {
		return 0.8 // Good match (3+ digits, likely same general area)
	} else if matchingDigits >= 1 {
		return 0.5 // Partial match (same region)
	} else {
		return 0.0
	}
}

func (f *ZipCodeSimilarity) Name() string {
	return "ZipCodeSimilarity"
}

// Helper function to get the last N characters of a string
func getLastN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
