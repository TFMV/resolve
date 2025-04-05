package similarity

import (
	"strings"
)

// Registry provides centralized access to different similarity functions for various field types
type Registry struct {
	// Field-specific comparators
	name    Function
	address Function
	phone   Function
	email   Function
	zipCode Function

	// Generic comparators
	text        Function
	exactMatch  Function
	caseMatch   Function
	jaroWinkler Function
	levenshtein Function
	jaccard     Function
	cosine      Function
	containedIn Function
}

// NewRegistry creates a new registry with all supported similarity functions
func NewRegistry() *Registry {
	return &Registry{
		// Field-specific comparators
		name:    NewNameSimilarity(),
		address: NewAddressSimilarity(),
		phone:   NewPhoneSimilarity(),
		email:   NewEmailSimilarity(),
		zipCode: NewZipCodeSimilarity(),

		// Generic comparators
		text:        NewJaroWinkler(), // Default text comparator
		exactMatch:  &ExactMatch{},
		caseMatch:   &CaseInsensitiveMatch{},
		jaroWinkler: NewJaroWinkler(),
		levenshtein: &Levenshtein{},
		jaccard:     &Jaccard{},
		cosine:      &Cosine{},
		containedIn: &ContainedIn{IgnoreCase: true},
	}
}

// GetByName returns a similarity function by name
func (r *Registry) GetByName(name string) Function {
	name = strings.ToLower(name)
	switch name {
	case "name", "namesimilarity":
		return r.name
	case "address", "addresssimilarity":
		return r.address
	case "phone", "phonesimilarity", "phonenumber":
		return r.phone
	case "email", "emailsimilarity":
		return r.email
	case "zipcode", "postalcode", "zip":
		return r.zipCode
	case "text", "default":
		return r.text
	case "exact", "exactmatch":
		return r.exactMatch
	case "case", "caseinsensitive", "caseinsensitivematch":
		return r.caseMatch
	case "jaro", "jarowinkler":
		return r.jaroWinkler
	case "levenshtein", "editdistance":
		return r.levenshtein
	case "jaccard", "token":
		return r.jaccard
	case "cosine", "cosinesimilarity":
		return r.cosine
	case "contains", "containedin":
		return r.containedIn
	default:
		// Default to text similarity
		return r.text
	}
}

// GetByFieldType returns the appropriate similarity function for a field type
func (r *Registry) GetByFieldType(fieldType string) Function {
	fieldType = strings.ToLower(fieldType)
	switch fieldType {
	case "name", "business_name", "person_name", "company", "organization":
		return r.name
	case "address", "street", "street_address", "mailing_address":
		return r.address
	case "phone", "phone_number", "telephone", "mobile", "cell", "fax":
		return r.phone
	case "email", "email_address":
		return r.email
	case "zip", "zipcode", "postal_code", "postal":
		return r.zipCode
	default:
		// Default to text similarity
		return r.text
	}
}

// Name returns the name similarity function
func (r *Registry) Name() Function {
	return r.name
}

// Address returns the address similarity function
func (r *Registry) Address() Function {
	return r.address
}

// Phone returns the phone similarity function
func (r *Registry) Phone() Function {
	return r.phone
}

// Email returns the email similarity function
func (r *Registry) Email() Function {
	return r.email
}

// ZipCode returns the zip code similarity function
func (r *Registry) ZipCode() Function {
	return r.zipCode
}

// Text returns the generic text similarity function
func (r *Registry) Text() Function {
	return r.text
}

// ExactMatch returns the exact match function
func (r *Registry) ExactMatch() Function {
	return r.exactMatch
}

// CaseInsensitiveMatch returns the case insensitive match function
func (r *Registry) CaseInsensitiveMatch() Function {
	return r.caseMatch
}

// JaroWinkler returns the Jaro-Winkler similarity function
func (r *Registry) JaroWinkler() Function {
	return r.jaroWinkler
}

// Levenshtein returns the Levenshtein similarity function
func (r *Registry) Levenshtein() Function {
	return r.levenshtein
}

// Jaccard returns the Jaccard similarity function
func (r *Registry) Jaccard() Function {
	return r.jaccard
}

// Cosine returns the Cosine similarity function
func (r *Registry) Cosine() Function {
	return r.cosine
}

// ContainedIn returns the ContainedIn similarity function
func (r *Registry) ContainedIn() Function {
	return r.containedIn
}
