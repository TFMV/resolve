package match

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/TFMV/resolve/internal/cluster"
	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/normalize"
	"github.com/TFMV/resolve/internal/similarity"
	"github.com/TFMV/resolve/internal/weaviate"
)

// EntityData represents a single entity with all its attributes
type EntityData struct {
	ID       string                 `json:"id,omitempty"`
	Fields   map[string]string      `json:"fields"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// FieldScore represents a similarity score for a specific field
type FieldScore struct {
	Score        float32 `json:"score"`
	QueryValue   string  `json:"query_value,omitempty"`
	MatchedValue string  `json:"matched_value,omitempty"`
	SimilarityFn string  `json:"similarity_function,omitempty"`
	Normalized   bool    `json:"normalized,omitempty"`
}

// MatchResult represents a match result with scores
type MatchResult struct {
	ID          string                 `json:"id"`
	Score       float32                `json:"score"`
	Fields      map[string]string      `json:"fields"`
	MatchedOn   []string               `json:"matched_on"`
	Explanation string                 `json:"explanation"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   int64                  `json:"created_at,omitempty"`
	UpdatedAt   int64                  `json:"updated_at,omitempty"`
	FieldScores map[string]FieldScore  `json:"field_scores,omitempty"`
}

// Options represents matching options
type Options struct {
	Limit                 int
	Threshold             float32
	IncludeDetails        bool
	UseClustering         bool               // Whether to use clustering
	IncludeFieldScores    bool               // Whether to include field-level similarity scores
	FieldWeights          map[string]float32 // Optional field weights for weighted scoring
	FieldTypeMappings     map[string]string  // Optional field type mappings for similarity functions
	ForceExactMatchFields []string           // Fields that should use exact matching
}

// Service represents the matching service
type Service struct {
	cfg              *config.Config
	normalizer       *normalize.Normalizer
	embeddingService embed.EmbeddingService
	weaviateClient   *weaviate.Client
	clusterService   *cluster.Service
	similarityReg    *similarity.Registry
}

// NewService creates a new matching service
func NewService(cfg *config.Config, weaviateClient *weaviate.Client, embeddingService embed.EmbeddingService) *Service {
	// Create normalizer
	normalizer := normalize.NewNormalizer(cfg)

	// Create cluster service
	clusterConfig := &cluster.Config{
		Enabled:             cfg.Clustering.Enabled,
		Method:              cfg.Clustering.Method,
		Fields:              cfg.Clustering.Fields,
		SimilarityThreshold: cfg.Clustering.SimilarityThreshold,
	}
	clusterService := cluster.NewService(clusterConfig, normalizer)

	// Create similarity registry
	similarityReg := similarity.NewRegistry()

	return &Service{
		cfg:              cfg,
		normalizer:       normalizer,
		embeddingService: embeddingService,
		weaviateClient:   weaviateClient,
		clusterService:   clusterService,
		similarityReg:    similarityReg,
	}
}

// AddEntity adds a single entity to the database
func (s *Service) AddEntity(ctx context.Context, data EntityData) error {
	// Normalize fields
	normalizedFields := s.normalizer.NormalizeEntity(data.Fields)

	// Concatenate fields for embedding
	textToEmbed := combineFields(normalizedFields)

	// Generate embeddings
	vector, err := s.embeddingService.GetEmbedding(ctx, textToEmbed)
	if err != nil {
		return fmt.Errorf("failed to generate embeddings: %w", err)
	}

	// Convert to Weaviate entity
	entity := convertToWeaviateEntity(data.ID, normalizedFields, vector, data.Metadata)

	// Assign cluster ID if clustering is enabled
	if s.cfg.Clustering.Enabled {
		_, err = s.clusterService.AssignCluster(ctx, entity)
		if err != nil {
			return fmt.Errorf("failed to assign cluster to entity: %w", err)
		}
	}

	// Add to Weaviate
	_, err = s.weaviateClient.AddEntity(ctx, entity)
	if err != nil {
		return fmt.Errorf("failed to add entity to Weaviate: %w", err)
	}

	return nil
}

// AddEntities adds multiple entities to the database in batch
func (s *Service) AddEntities(ctx context.Context, dataList []EntityData) error {
	entities := make([]*weaviate.EntityRecord, len(dataList))

	// Process all entities first (normalize & generate embeddings)
	for i, data := range dataList {
		// Normalize fields
		normalizedFields := s.normalizer.NormalizeEntity(data.Fields)

		// Concatenate fields for embedding
		textToEmbed := combineFields(normalizedFields)

		// Generate embedding
		vector, err := s.embeddingService.GetEmbedding(ctx, textToEmbed)
		if err != nil {
			return fmt.Errorf("failed to generate embeddings for entity %d: %w", i, err)
		}

		// Convert to Weaviate entity
		entities[i] = convertToWeaviateEntity(data.ID, normalizedFields, vector, data.Metadata)

		// Assign cluster ID if clustering is enabled
		if s.cfg.Clustering.Enabled {
			_, err = s.clusterService.AssignCluster(ctx, entities[i])
			if err != nil {
				return fmt.Errorf("failed to assign cluster to entity %d: %w", i, err)
			}
		}
	}

	// Add to Weaviate in batch
	_, err := s.weaviateClient.BatchAddEntities(ctx, entities)
	if err != nil {
		return fmt.Errorf("failed to add entities to Weaviate: %w", err)
	}

	return nil
}

// FindMatches finds the best matching entities for the input text
func (s *Service) FindMatches(ctx context.Context, text string, opts Options) ([]MatchResult, error) {
	// Apply default options if needed
	if opts.Limit <= 0 {
		opts.Limit = s.cfg.Matching.DefaultLimit
	}

	if opts.Threshold <= 0 {
		opts.Threshold = s.cfg.Matching.SimilarityThreshold
	}

	// Default to using clustering if enabled and not explicitly disabled
	if !opts.UseClustering {
		opts.UseClustering = s.cfg.Clustering.Enabled
	}

	// Generate embedding for the query
	vector, err := s.embeddingService.GetEmbedding(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding for query: %w", err)
	}

	// Create a temporary entity to assign a cluster
	tempEntity := &weaviate.EntityRecord{
		Name:   text,
		Vector: vector,
	}

	// Get cluster filter if clustering is enabled and we should use it
	var filterParams map[string]string
	if opts.UseClustering && s.cfg.Clustering.Enabled {
		_, err = s.clusterService.AssignCluster(ctx, tempEntity)
		if err != nil {
			return nil, fmt.Errorf("failed to assign cluster to query: %w", err)
		}

		filterParams = s.clusterService.GetClusterFilterForEntity(ctx, tempEntity)
	}

	// Double the limit to account for filtering effect of clustering
	var searchLimit int
	if opts.UseClustering && s.cfg.Clustering.Enabled {
		searchLimit = opts.Limit * 3 // Get more candidates to compensate for cluster filtering
	} else {
		searchLimit = opts.Limit
	}

	// Search in Weaviate
	results, err := s.weaviateClient.SearchEntities(ctx, vector, searchLimit, filterParams)
	if err != nil {
		return nil, fmt.Errorf("failed to search Weaviate: %w", err)
	}

	// Parse input fields if text contains field=value pairs
	queryFields := parseQueryFields(text)

	// Convert to match results
	matchResults := make([]MatchResult, 0, len(results))
	for _, result := range results {
		// Get score from metadata (distance is stored there by Weaviate client)
		score := float32(1.0) // Default score
		if result.Metadata != nil {
			if distVal, ok := result.Metadata["distance"].(float64); ok {
				// Convert distance to similarity score (1 - distance)
				score = float32(1.0 - distVal)
			}
		}

		// Skip results with a score below threshold
		if score < opts.Threshold {
			continue
		}

		// Convert to match result
		matchResult := convertToMatchResult(result, score)

		// Apply field-level scoring if requested
		if opts.IncludeFieldScores || len(queryFields) > 0 {
			s.computeFieldScores(&matchResult, queryFields, opts)
		}

		matchResults = append(matchResults, matchResult)
	}

	// Sort by score descending
	sort.Slice(matchResults, func(i, j int) bool {
		return matchResults[i].Score > matchResults[j].Score
	})

	// Apply limit after final sorting
	if len(matchResults) > opts.Limit {
		matchResults = matchResults[:opts.Limit]
	}

	return matchResults, nil
}

// FindMatchesForEntity finds the best matching entities for the given entity
func (s *Service) FindMatchesForEntity(ctx context.Context, entity EntityData, opts Options) ([]MatchResult, error) {
	// Apply default options if needed
	if opts.Limit <= 0 {
		opts.Limit = s.cfg.Matching.DefaultLimit
	}

	if opts.Threshold <= 0 {
		opts.Threshold = s.cfg.Matching.SimilarityThreshold
	}

	// Normalize fields
	normalizedFields := s.normalizer.NormalizeEntity(entity.Fields)

	// Concatenate fields for embedding
	textToEmbed := combineFields(normalizedFields)

	// Then use the regular FindMatches method
	return s.FindMatches(ctx, textToEmbed, opts)
}

// computeFieldScores calculates and adds field-level similarity scores to the match result
func (s *Service) computeFieldScores(result *MatchResult, queryFields map[string]string, opts Options) {
	// Initialize field scores map if needed
	if result.FieldScores == nil {
		result.FieldScores = make(map[string]FieldScore)
	}

	// If queryFields is empty, compare with all fields in the match
	if len(queryFields) == 0 {
		// Use match fields directly
		for fieldName, fieldValue := range result.Fields {
			// Skip empty fields
			if fieldValue == "" {
				continue
			}

			// Get field similarity function
			var simFn similarity.Function
			if fieldType, ok := opts.FieldTypeMappings[fieldName]; ok {
				simFn = s.similarityReg.GetByFieldType(fieldType)
			} else {
				// Infer field type from name
				simFn = s.inferSimilarityFunction(fieldName)
			}

			// Check if this field should use exact matching
			for _, exactField := range opts.ForceExactMatchFields {
				if exactField == fieldName {
					simFn = s.similarityReg.ExactMatch()
					break
				}
			}

			// Calculate field score - compare with itself since we don't have a query value
			score := float32(simFn.Compare(fieldValue, fieldValue))

			// Add to field scores
			result.FieldScores[fieldName] = FieldScore{
				Score:        score,
				MatchedValue: fieldValue,
				SimilarityFn: simFn.Name(),
				Normalized:   true,
			}
		}
	} else {
		// Compare query fields with match fields
		for queryField, queryValue := range queryFields {
			// Skip if query value is empty
			if queryValue == "" {
				continue
			}

			// Get the corresponding field value from the match
			matchValue, ok := result.Fields[queryField]
			if !ok {
				// Field not found in match, skip
				continue
			}

			// Get field similarity function
			var simFn similarity.Function
			if fieldType, ok := opts.FieldTypeMappings[queryField]; ok {
				simFn = s.similarityReg.GetByFieldType(fieldType)
			} else {
				// Infer field type from name
				simFn = s.inferSimilarityFunction(queryField)
			}

			// Check if this field should use exact matching
			for _, exactField := range opts.ForceExactMatchFields {
				if exactField == queryField {
					simFn = s.similarityReg.ExactMatch()
					break
				}
			}

			// Calculate field score
			score := float32(simFn.Compare(queryValue, matchValue))

			// Add to field scores
			result.FieldScores[queryField] = FieldScore{
				Score:        score,
				QueryValue:   queryValue,
				MatchedValue: matchValue,
				SimilarityFn: simFn.Name(),
				Normalized:   true,
			}
		}
	}

	// Optionally update the overall score if field weights are provided
	if len(opts.FieldWeights) > 0 && len(result.FieldScores) > 0 {
		weightedScore := computeWeightedScore(result.FieldScores, opts.FieldWeights)

		// Blend the vector score with the field-level score
		// Default to equal weighting
		result.Score = (result.Score + weightedScore) / 2
	}
}

// inferSimilarityFunction infers the appropriate similarity function for a field based on its name
func (s *Service) inferSimilarityFunction(fieldName string) similarity.Function {
	fieldNameLower := strings.ToLower(fieldName)

	// Check if the field name contains common indicators
	if strings.Contains(fieldNameLower, "name") ||
		strings.Contains(fieldNameLower, "company") ||
		strings.Contains(fieldNameLower, "business") ||
		strings.Contains(fieldNameLower, "organization") {
		return s.similarityReg.Name()
	}

	if strings.Contains(fieldNameLower, "address") ||
		strings.Contains(fieldNameLower, "street") {
		return s.similarityReg.Address()
	}

	if strings.Contains(fieldNameLower, "phone") ||
		strings.Contains(fieldNameLower, "tel") ||
		strings.Contains(fieldNameLower, "mobile") ||
		strings.Contains(fieldNameLower, "cell") ||
		strings.Contains(fieldNameLower, "fax") {
		return s.similarityReg.Phone()
	}

	if strings.Contains(fieldNameLower, "email") {
		return s.similarityReg.Email()
	}

	if strings.Contains(fieldNameLower, "zip") ||
		strings.Contains(fieldNameLower, "postal") {
		return s.similarityReg.ZipCode()
	}

	// Default to generic text similarity
	return s.similarityReg.Text()
}

// parseQueryFields attempts to parse field=value pairs from the input text
// Format: field1=value1;field2=value2;...
func parseQueryFields(text string) map[string]string {
	fields := make(map[string]string)

	// Check if the text contains field=value format
	if !strings.Contains(text, "=") {
		return fields
	}

	// Split by semicolon or comma
	var pairs []string
	if strings.Contains(text, ";") {
		pairs = strings.Split(text, ";")
	} else if strings.Contains(text, ",") {
		pairs = strings.Split(text, ",")
	} else {
		// Single pair
		pairs = []string{text}
	}

	// Process each pair
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Split by equals sign
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}

		fieldName := strings.TrimSpace(parts[0])
		fieldValue := strings.TrimSpace(parts[1])

		if fieldName != "" && fieldValue != "" {
			fields[fieldName] = fieldValue
		}
	}

	return fields
}

// computeWeightedScore calculates a weighted score based on field-level scores and weights
func computeWeightedScore(fieldScores map[string]FieldScore, fieldWeights map[string]float32) float32 {
	var totalScore float32
	var totalWeight float32

	for fieldName, fieldScore := range fieldScores {
		weight, ok := fieldWeights[fieldName]
		if !ok {
			// Use default weight if not specified
			weight = 1.0
		}

		totalScore += fieldScore.Score * weight
		totalWeight += weight
	}

	// Avoid division by zero
	if totalWeight == 0 {
		return 0
	}

	return totalScore / totalWeight
}

// convertToWeaviateEntity converts EntityData to a Weaviate entity record
func convertToWeaviateEntity(id string, fields map[string]string, vector []float32, metadata map[string]interface{}) *weaviate.EntityRecord {
	// Create a new entity record
	entity := &weaviate.EntityRecord{
		ID:       id,
		Vector:   vector,
		Metadata: metadata,
	}

	// Add timestamps if not present in metadata
	now := time.Now().Unix()
	if entity.Metadata == nil {
		entity.Metadata = make(map[string]interface{})
	}
	if _, ok := entity.Metadata["created_at"]; !ok {
		entity.Metadata["created_at"] = now
	}
	if _, ok := entity.Metadata["updated_at"]; !ok {
		entity.Metadata["updated_at"] = now
	}

	// Map standard fields to the entity
	if name, ok := fields["name"]; ok {
		entity.Name = name
	}
	if name, ok := fields["name_normalized"]; ok {
		entity.NameNormalized = name
	}
	if address, ok := fields["address"]; ok {
		entity.Address = address
	}
	if address, ok := fields["address_normalized"]; ok {
		entity.AddressNormalized = address
	}
	if city, ok := fields["city"]; ok {
		entity.City = city
	}
	if city, ok := fields["city_normalized"]; ok {
		entity.CityNormalized = city
	}
	if state, ok := fields["state"]; ok {
		entity.State = state
	}
	if state, ok := fields["state_normalized"]; ok {
		entity.StateNormalized = state
	}
	if zip, ok := fields["zip"]; ok {
		entity.Zip = zip
	}
	if zip, ok := fields["zip_normalized"]; ok {
		entity.ZipNormalized = zip
	}
	if phone, ok := fields["phone"]; ok {
		entity.Phone = phone
	}
	if phone, ok := fields["phone_normalized"]; ok {
		entity.PhoneNormalized = phone
	}
	if email, ok := fields["email"]; ok {
		entity.Email = email
	}
	if email, ok := fields["email_normalized"]; ok {
		entity.EmailNormalized = email
	}

	return entity
}

// convertToMatchResult converts a Weaviate EntityRecord to a MatchResult
func convertToMatchResult(entity *weaviate.EntityRecord, score float32) MatchResult {
	// Create fields map from the entity's fields
	fields := map[string]string{
		"name":    entity.Name,
		"address": entity.Address,
		"city":    entity.City,
		"state":   entity.State,
		"zip":     entity.Zip,
		"phone":   entity.Phone,
		"email":   entity.Email,
	}

	// Add normalized fields if available
	if entity.NameNormalized != "" {
		fields["name_normalized"] = entity.NameNormalized
	}
	if entity.AddressNormalized != "" {
		fields["address_normalized"] = entity.AddressNormalized
	}
	if entity.CityNormalized != "" {
		fields["city_normalized"] = entity.CityNormalized
	}
	if entity.StateNormalized != "" {
		fields["state_normalized"] = entity.StateNormalized
	}
	if entity.ZipNormalized != "" {
		fields["zip_normalized"] = entity.ZipNormalized
	}
	if entity.PhoneNormalized != "" {
		fields["phone_normalized"] = entity.PhoneNormalized
	}
	if entity.EmailNormalized != "" {
		fields["email_normalized"] = entity.EmailNormalized
	}

	// Extract timestamps from metadata if available
	var createdAt, updatedAt int64
	if entity.Metadata != nil {
		if val, ok := entity.Metadata["created_at"]; ok {
			if timestamp, ok := val.(float64); ok {
				createdAt = int64(timestamp)
			}
		}
		if val, ok := entity.Metadata["updated_at"]; ok {
			if timestamp, ok := val.(float64); ok {
				updatedAt = int64(timestamp)
			}
		}
	}

	// Get matched fields for explanation
	matchedOn := getMatchedFields(fields)
	explanation := generateExplanation(score, matchedOn)

	return MatchResult{
		ID:          entity.ID,
		Score:       score,
		Fields:      fields,
		MatchedOn:   matchedOn,
		Explanation: explanation,
		Metadata:    entity.Metadata,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		FieldScores: make(map[string]FieldScore),
	}
}

// getMatchedFields determines which fields contributed to matching
// This is a heuristic since the exact match details are not provided by Weaviate
func getMatchedFields(fields map[string]string) []string {
	var matchedFields []string
	for field, value := range fields {
		if value != "" && !strings.HasSuffix(field, "_normalized") {
			matchedFields = append(matchedFields, field)
		}
	}
	return matchedFields
}

// generateExplanation creates a human-readable explanation of the match
func generateExplanation(score float32, matchedFields []string) string {
	confidence := "medium"
	if score >= 0.9 {
		confidence = "high"
	} else if score < 0.7 {
		confidence = "low"
	}

	return fmt.Sprintf("Matched with %s confidence (%0.2f) on fields: %s",
		confidence, score, strings.Join(matchedFields, ", "))
}

// combineFields concatenates field values for embedding
func combineFields(fields map[string]string) string {
	var values []string
	for _, value := range fields {
		if value != "" {
			values = append(values, value)
		}
	}
	return strings.Join(values, " ")
}

// RecomputeClusters recomputes clusters for all entities
func (s *Service) RecomputeClusters(ctx context.Context) error {
	if !s.cfg.Clustering.Enabled {
		return fmt.Errorf("clustering is not enabled in the configuration")
	}

	batchSize := 100 // Process entities in batches
	return s.clusterService.RecomputeAllClusters(ctx, s.weaviateClient, batchSize)
}
