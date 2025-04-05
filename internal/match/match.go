package match

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/TFMV/resolve/internal/cluster"
	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/normalize"
	"github.com/TFMV/resolve/internal/weaviate"
)

// EntityData represents a single entity with all its attributes
type EntityData struct {
	ID       string                 `json:"id,omitempty"`
	Fields   map[string]string      `json:"fields"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
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
	FieldScores map[string]float32     `json:"field_scores,omitempty"`
}

// Options represents matching options
type Options struct {
	Limit          int
	Threshold      float32
	IncludeDetails bool
	UseClustering  bool // Whether to use clustering
}

// Service represents the matching service
type Service struct {
	cfg              *config.Config
	normalizer       *normalize.Normalizer
	embeddingService embed.EmbeddingService
	weaviateClient   *weaviate.Client
	clusterService   *cluster.Service
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

	return &Service{
		cfg:              cfg,
		normalizer:       normalizer,
		embeddingService: embeddingService,
		weaviateClient:   weaviateClient,
		clusterService:   clusterService,
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

	// If no results with cluster filtering and clustering is enabled, try again without filtering
	if len(results) == 0 && opts.UseClustering && s.cfg.Clustering.Enabled {
		results, err = s.weaviateClient.SearchEntities(ctx, vector, searchLimit, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to search Weaviate with fallback: %w", err)
		}
	}

	// Convert and filter results
	matches := make([]MatchResult, 0, len(results))
	for _, result := range results {
		// Calculate confidence score from distance (1 - distance)
		// Weaviate uses cosine distance, so 0 means identical vectors, 2 means opposite vectors
		score := float32(1.0)

		// Skip if below threshold
		if score < opts.Threshold {
			continue
		}

		// Create match result
		match := convertToMatchResult(result, score, opts.IncludeDetails)
		matches = append(matches, match)
	}

	// Limit the final results
	if len(matches) > opts.Limit {
		matches = matches[:opts.Limit]
	}

	return matches, nil
}

// FindMatchesForEntity finds the best matching entities for an entity
func (s *Service) FindMatchesForEntity(ctx context.Context, data EntityData, opts Options) ([]MatchResult, error) {
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

	// Normalize fields
	normalizedFields := s.normalizer.NormalizeEntity(data.Fields)

	// Concatenate fields for embedding
	textToEmbed := combineFields(normalizedFields)

	// Generate embedding
	vector, err := s.embeddingService.GetEmbedding(ctx, textToEmbed)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding for query: %w", err)
	}

	// Create entity record for clustering
	entity := convertToWeaviateEntity(data.ID, normalizedFields, vector, data.Metadata)

	// Get cluster filter if clustering is enabled
	var filterParams map[string]string
	if opts.UseClustering && s.cfg.Clustering.Enabled {
		_, err = s.clusterService.AssignCluster(ctx, entity)
		if err != nil {
			return nil, fmt.Errorf("failed to assign cluster to query entity: %w", err)
		}

		filterParams = s.clusterService.GetClusterFilterForEntity(ctx, entity)
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

	// If no results with cluster filtering and clustering is enabled, try again without filtering
	if len(results) == 0 && opts.UseClustering && s.cfg.Clustering.Enabled {
		results, err = s.weaviateClient.SearchEntities(ctx, vector, searchLimit, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to search Weaviate with fallback: %w", err)
		}
	}

	// Convert and filter results
	matches := make([]MatchResult, 0, len(results))
	for _, result := range results {
		// Skip if it's the same entity
		if data.ID != "" && data.ID == result.ID {
			continue
		}

		// Calculate confidence score from distance (1 - distance)
		score := float32(1.0)

		// Skip if below threshold
		if score < opts.Threshold {
			continue
		}

		// Create match result
		match := convertToMatchResult(result, score, opts.IncludeDetails)
		matches = append(matches, match)
	}

	// Limit the final results
	if len(matches) > opts.Limit {
		matches = matches[:opts.Limit]
	}

	return matches, nil
}

// RecomputeClusters recomputes clusters for all entities
func (s *Service) RecomputeClusters(ctx context.Context) error {
	if !s.cfg.Clustering.Enabled {
		return fmt.Errorf("clustering is not enabled in the configuration")
	}

	batchSize := 100 // Process entities in batches
	return s.clusterService.RecomputeAllClusters(ctx, s.weaviateClient, batchSize)
}

// GetEntityCount gets the total count of entities in the database
func (s *Service) GetEntityCount(ctx context.Context) (int, error) {
	return s.weaviateClient.GetCount(ctx)
}

// combineFields combines normalized fields into a single string for embedding
func combineFields(fields map[string]string) string {
	result := ""

	// Order matters for deterministic embeddings
	keys := []string{"name_normalized", "address_normalized", "city_normalized", "state_normalized", "zip_normalized", "phone_normalized", "email_normalized"}

	for _, key := range keys {
		if value, exists := fields[key]; exists && value != "" {
			if result != "" {
				result += " "
			}
			result += value
		}
	}

	return result
}

// convertToWeaviateEntity converts a normalized entity to Weaviate's format
func convertToWeaviateEntity(id string, fields map[string]string, vector []float32, metadata map[string]interface{}) *weaviate.EntityRecord {
	entity := &weaviate.EntityRecord{
		ID:        id,
		Vector:    vector,
		Metadata:  metadata,
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}

	// Copy fields
	if name, exists := fields["name"]; exists {
		entity.Name = name
	}
	if normalizedName, exists := fields["name_normalized"]; exists {
		entity.NameNormalized = normalizedName
	}
	if address, exists := fields["address"]; exists {
		entity.Address = address
	}
	if normalizedAddress, exists := fields["address_normalized"]; exists {
		entity.AddressNormalized = normalizedAddress
	}
	if city, exists := fields["city"]; exists {
		entity.City = city
	}
	if normalizedCity, exists := fields["city_normalized"]; exists {
		entity.CityNormalized = normalizedCity
	}
	if state, exists := fields["state"]; exists {
		entity.State = state
	}
	if normalizedState, exists := fields["state_normalized"]; exists {
		entity.StateNormalized = normalizedState
	}
	if zip, exists := fields["zip"]; exists {
		entity.Zip = zip
	}
	if normalizedZip, exists := fields["zip_normalized"]; exists {
		entity.ZipNormalized = normalizedZip
	}
	if phone, exists := fields["phone"]; exists {
		entity.Phone = phone
	}
	if normalizedPhone, exists := fields["phone_normalized"]; exists {
		entity.PhoneNormalized = normalizedPhone
	}
	if email, exists := fields["email"]; exists {
		entity.Email = email
	}
	if normalizedEmail, exists := fields["email_normalized"]; exists {
		entity.EmailNormalized = normalizedEmail
	}

	return entity
}

// convertToMatchResult converts a Weaviate entity to a match result
func convertToMatchResult(entity *weaviate.EntityRecord, score float32, includeDetails bool) MatchResult {
	// Create fields map
	fields := map[string]string{
		"name":    entity.Name,
		"address": entity.Address,
		"city":    entity.City,
		"state":   entity.State,
		"zip":     entity.Zip,
		"phone":   entity.Phone,
		"email":   entity.Email,
	}

	// Create match result
	match := MatchResult{
		ID:        entity.ID,
		Score:     score,
		Fields:    fields,
		Metadata:  entity.Metadata,
		CreatedAt: entity.CreatedAt,
		UpdatedAt: entity.UpdatedAt,
	}

	// Add field scores if details are requested
	if includeDetails {
		match.FieldScores = map[string]float32{
			"name":    1.0,
			"address": 1.0,
			"city":    1.0,
			"state":   1.0,
			"zip":     1.0,
			"phone":   1.0,
			"email":   1.0,
		}
	}

	return match
}

// determineMatchedFields determines which fields matched between entities
func determineMatchedFields(query map[string]string, result *weaviate.EntityRecord) []string {
	matchedFields := make([]string, 0)

	// Check name
	if queryName, exists := query["name_normalized"]; exists && queryName != "" {
		if result.NameNormalized == queryName {
			matchedFields = append(matchedFields, "name")
		}
	}

	// Check address
	if queryAddr, exists := query["address_normalized"]; exists && queryAddr != "" {
		if result.AddressNormalized == queryAddr {
			matchedFields = append(matchedFields, "address")
		}
	}

	// Check city
	if queryCity, exists := query["city_normalized"]; exists && queryCity != "" {
		if result.CityNormalized == queryCity {
			matchedFields = append(matchedFields, "city")
		}
	}

	// Check state
	if queryState, exists := query["state_normalized"]; exists && queryState != "" {
		if result.StateNormalized == queryState {
			matchedFields = append(matchedFields, "state")
		}
	}

	// Check zip
	if queryZip, exists := query["zip_normalized"]; exists && queryZip != "" {
		if result.ZipNormalized == queryZip {
			matchedFields = append(matchedFields, "zip")
		}
	}

	// Check phone
	if queryPhone, exists := query["phone_normalized"]; exists && queryPhone != "" {
		if result.PhoneNormalized == queryPhone {
			matchedFields = append(matchedFields, "phone")
		}
	}

	// Check email
	if queryEmail, exists := query["email_normalized"]; exists && queryEmail != "" {
		if result.EmailNormalized == queryEmail {
			matchedFields = append(matchedFields, "email")
		}
	}

	// Sort fields
	sort.Strings(matchedFields)

	return matchedFields
}

// generateExplanation generates a human-readable explanation of the match
func generateExplanation(matchedFields []string, fieldScores map[string]float32) string {
	if len(matchedFields) == 0 {
		return "Matched based on overall similarity"
	}

	if len(matchedFields) == 1 {
		return fmt.Sprintf("Matched on %s field", matchedFields[0])
	}

	lastField := matchedFields[len(matchedFields)-1]
	otherFields := matchedFields[:len(matchedFields)-1]

	return fmt.Sprintf("Matched on %s and %s fields", joinFields(otherFields), lastField)
}

// joinFields joins field names with commas
func joinFields(fields []string) string {
	result := ""

	for i, field := range fields {
		if i > 0 {
			result += ", "
		}
		result += field
	}

	return result
}
