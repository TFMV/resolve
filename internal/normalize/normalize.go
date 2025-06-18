package normalize

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/TFMV/resolve/internal/config"
)

// Normalizer provides methods to normalize entity fields
type Normalizer struct {
	cfg                  *config.Config
	legalSuffixRegex     *regexp.Regexp
	addressRegex         *regexp.Regexp
	phoneRegex           *regexp.Regexp
	emailRegex           *regexp.Regexp
	spaceRegex           *regexp.Regexp
	initialsRegex        *regexp.Regexp
	apartmentRegex       *regexp.Regexp
	nonAlphanumericRegex *regexp.Regexp
	streetAbbreviations  map[string]string
	stateCodes           map[string]string
	stopwords            map[string]bool
}

// NewNormalizer creates a new normalizer with the given configuration
func NewNormalizer(cfg *config.Config) *Normalizer {
	n := &Normalizer{
		cfg:                  cfg,
		legalSuffixRegex:     regexp.MustCompile(`(?i)\s+(inc\.?|incorporated|corp\.?|corporation|llc|ltd\.?|limited|llp|l\.l\.p\.?|pllc|p\.l\.l\.c\.?|pc|p\.c\.?)$`),
		addressRegex:         regexp.MustCompile(`(?i)(\d+)\s+([a-z0-9\.\-\s]+)\s+(st|street|ave|avenue|blvd|boulevard|rd|road|ln|lane|way|dr|drive|court|ct|plaza|square|sq|parkway|pkwy)\.?`),
		phoneRegex:           regexp.MustCompile(`^(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})$`),
		emailRegex:           regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`),
		spaceRegex:           regexp.MustCompile(`\s+`),
		initialsRegex:        regexp.MustCompile(`\b([A-Z])\.?\b`),
		apartmentRegex:       regexp.MustCompile(`(?i)(\s+)(apt|apartment|ste|suite|unit|#)\.?\s+[a-z0-9-]+`),
		nonAlphanumericRegex: regexp.MustCompile(`[^0-9a-zA-Z]`),
		streetAbbreviations: map[string]string{
			"street":    "st",
			"avenue":    "ave",
			"boulevard": "blvd",
			"road":      "rd",
			"lane":      "ln",
			"drive":     "dr",
			"court":     "ct",
			"square":    "sq",
			"parkway":   "pkwy",
		},
		stateCodes: map[string]string{
			"alabama":        "AL",
			"alaska":         "AK",
			"arizona":        "AZ",
			"arkansas":       "AR",
			"california":     "CA",
			"colorado":       "CO",
			"connecticut":    "CT",
			"delaware":       "DE",
			"florida":        "FL",
			"georgia":        "GA",
			"hawaii":         "HI",
			"idaho":          "ID",
			"illinois":       "IL",
			"indiana":        "IN",
			"iowa":           "IA",
			"kansas":         "KS",
			"kentucky":       "KY",
			"louisiana":      "LA",
			"maine":          "ME",
			"maryland":       "MD",
			"massachusetts":  "MA",
			"michigan":       "MI",
			"minnesota":      "MN",
			"mississippi":    "MS",
			"missouri":       "MO",
			"montana":        "MT",
			"nebraska":       "NE",
			"nevada":         "NV",
			"new hampshire":  "NH",
			"new jersey":     "NJ",
			"new mexico":     "NM",
			"new york":       "NY",
			"north carolina": "NC",
			"north dakota":   "ND",
			"ohio":           "OH",
			"oklahoma":       "OK",
			"oregon":         "OR",
			"pennsylvania":   "PA",
			"rhode island":   "RI",
			"south carolina": "SC",
			"south dakota":   "SD",
			"tennessee":      "TN",
			"texas":          "TX",
			"utah":           "UT",
			"vermont":        "VT",
			"virginia":       "VA",
			"washington":     "WA",
			"west virginia":  "WV",
			"wisconsin":      "WI",
			"wyoming":        "WY",
		},
		stopwords: map[string]bool{
			"a": true, "an": true, "the": true, "and": true, "but": true,
			"if": true, "or": true, "because": true, "as": true, "until": true,
			"while": true, "of": true, "at": true, "by": true, "for": true,
			"with": true, "about": true, "against": true, "between": true,
			"into": true, "through": true, "during": true, "before": true,
			"after": true, "above": true, "below": true, "to": true,
			"from": true, "up": true, "down": true, "in": true, "out": true,
			"on": true, "off": true, "over": true, "under": true, "again": true,
			"further": true, "then": true, "once": true, "here": true,
			"there": true, "when": true, "where": true, "why": true,
			"how": true, "all": true, "any": true, "both": true, "each": true,
			"few": true, "more": true, "most": true, "other": true,
			"some": true, "such": true, "no": true, "nor": true, "not": true,
			"only": true, "own": true, "same": true, "so": true, "than": true,
			"too": true, "very": true, "can": true, "will": true, "just": true,
			"should": true, "now": true,
		},
	}

	return n
}

// NormalizeText performs basic text normalization
func (n *Normalizer) NormalizeText(text string) string {
	if text == "" {
		return ""
	}

	// Convert to lowercase if enabled
	if n.cfg.Normalization.EnableLowercase {
		text = strings.ToLower(text)
	}

	// Remove extra whitespace
	text = strings.TrimSpace(text)
	text = n.spaceRegex.ReplaceAllString(text, " ")

	// Remove stopwords if enabled
	if n.cfg.Normalization.EnableStopwords {
		words := strings.Fields(text)
		filtered := make([]string, 0, len(words))

		for _, word := range words {
			if !n.stopwords[strings.ToLower(word)] {
				filtered = append(filtered, word)
			}
		}

		text = strings.Join(filtered, " ")
	}

	return text
}

// NormalizeName normalizes a business or personal name
func (n *Normalizer) NormalizeName(name string) string {
	if name == "" {
		return ""
	}

	// Apply basic text normalization
	name = n.NormalizeText(name)

	// Remove legal suffixes if enabled
	if n.cfg.Normalization.NameOptions["remove_legal_suffixes"] {
		name = n.legalSuffixRegex.ReplaceAllString(name, "")
	}

	// Normalize initials
	if n.cfg.Normalization.NameOptions["normalize_initials"] {
		name = n.initialsRegex.ReplaceAllString(name, "$1")
	}

	return strings.TrimSpace(name)
}

// NormalizeAddress standardizes an address string
func (n *Normalizer) NormalizeAddress(address string) string {
	if address == "" {
		return ""
	}

	// Apply basic text normalization
	address = n.NormalizeText(address)

	// Standardize abbreviations
	if n.cfg.Normalization.AddressOptions["standardize_abbreviations"] {
		for word, abbr := range n.streetAbbreviations {
			re := regexp.MustCompile(`(?i)\b` + word + `\b\.?`)
			address = re.ReplaceAllString(address, abbr)
		}
	}

	// Remove apartment/suite numbers
	if n.cfg.Normalization.AddressOptions["remove_apartment_numbers"] {
		address = n.apartmentRegex.ReplaceAllString(address, "")
	}

	return strings.TrimSpace(address)
}

// NormalizePhone converts phone numbers to E.164 format
func (n *Normalizer) NormalizePhone(phone string) string {
	if phone == "" {
		return ""
	}

	// If already in E.164 format, return as is
	if strings.HasPrefix(phone, "+") && len(phone) >= 8 && len(phone) <= 15 {
		return phone
	}

	// Extract parts of the phone number
	matches := n.phoneRegex.FindStringSubmatch(phone)
	if matches == nil {
		return phone // Return original if no match
	}

	// Default country code to 1 (US) if not provided
	countryCode := matches[1]
	if countryCode == "" {
		countryCode = "1"
	}

	// Normalize to E.164 format if enabled
	if n.cfg.Normalization.PhoneOptions["e164_format"] {
		return "+" + countryCode + matches[2] + matches[3] + matches[4]
	}

	return phone
}

// NormalizeEmail standardizes email addresses
func (n *Normalizer) NormalizeEmail(email string) string {
	if email == "" {
		return ""
	}

	// Validate email format
	if !n.emailRegex.MatchString(email) {
		return email // Return original if invalid
	}

	// Convert to lowercase if enabled
	if n.cfg.Normalization.EmailOptions["lowercase_domain"] {
		parts := strings.Split(email, "@")
		if len(parts) == 2 {
			return parts[0] + "@" + strings.ToLower(parts[1])
		}
	}

	return email
}

// NormalizeState converts state names to standard 2-letter codes
func (n *Normalizer) NormalizeState(state string) string {
	if state == "" {
		return ""
	}

	// Convert to lowercase for matching
	stateLower := strings.ToLower(state)

	// If already a valid 2-letter code, return uppercase
	if len(state) == 2 {
		return strings.ToUpper(state)
	}

	// Try to match with known state names
	if code, exists := n.stateCodes[stateLower]; exists {
		return code
	}

	return state
}

// NormalizeZip standardizes ZIP codes
func (n *Normalizer) NormalizeZip(zip string) string {
	if zip == "" {
		return ""
	}

	// Remove any non-alphanumeric characters
	zip = n.nonAlphanumericRegex.ReplaceAllString(zip, "")

	// For US ZIP codes, take the first 5 digits
	if len(zip) >= 5 && unicode.IsDigit(rune(zip[0])) {
		return zip[:5]
	}

	return zip
}

// NormalizeEntity applies normalization to all fields of an entity map
func (n *Normalizer) NormalizeEntity(entity map[string]string) map[string]string {
	normalized := make(map[string]string)

	// Copy original values
	for k, v := range entity {
		normalized[k] = v
	}

	// Apply specific normalizations
	if name, exists := entity["name"]; exists {
		normalized["name_normalized"] = n.NormalizeName(name)
	}

	if address, exists := entity["address"]; exists {
		normalized["address_normalized"] = n.NormalizeAddress(address)
	}

	if phone, exists := entity["phone"]; exists {
		normalized["phone_normalized"] = n.NormalizePhone(phone)
	}

	if email, exists := entity["email"]; exists {
		normalized["email_normalized"] = n.NormalizeEmail(email)
	}

	if state, exists := entity["state"]; exists {
		normalized["state_normalized"] = n.NormalizeState(state)
	}

	if zip, exists := entity["zip"]; exists {
		normalized["zip_normalized"] = n.NormalizeZip(zip)
	}

	if city, exists := entity["city"]; exists {
		normalized["city_normalized"] = n.NormalizeText(city)
	}

	return normalized
}
