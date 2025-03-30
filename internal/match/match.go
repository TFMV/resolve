package match

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/TFMV/resolve/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/normalize"
	"github.com/TFMV/resolve/internal/qdrant"
)

// Service represents the matching service
type Service struct {
	config           *config.Config
	qdrantClient     *qdrant.Client
	embeddingService embed.EmbeddingService
}

// Options represents matching options
type Options struct {
	// Limit is the maximum number of matches to return
	Limit int

	// Threshold is the minimum similarity score (0-1) required for a match
	Threshold float32

	// IncludeDetails indicates whether to include normalization details
	IncludeDetails bool
}

// Match represents a matching result
type Match struct {
	ID            string            `json:"id"`
	OriginalText  string            `json:"original_text"`
	Normalized    string            `json:"normalized"`
	Score         float32           `json:"score"`
	NormalizeInfo *normalize.Result `json:"normalize_info,omitempty"`
}

// EntityData represents input entity data
type EntityData struct {
	ID           string `json:"id"`
	OriginalText string `json:"original_text"`
}

// NewService creates a new matching service
func NewService(cfg *config.Config, qdrantClient *qdrant.Client, embeddingService embed.EmbeddingService) *Service {
	return &Service{
		config:           cfg,
		qdrantClient:     qdrantClient,
		embeddingService: embeddingService,
	}
}

// FindMatches finds entities that match the input string
func (s *Service) FindMatches(ctx context.Context, text string, opts Options) ([]Match, error) {
	if text == "" {
		return nil, fmt.Errorf("empty input text")
	}

	// Set default values if not provided
	if opts.Limit <= 0 {
		opts.Limit = 10
	}

	if opts.Threshold <= 0 {
		opts.Threshold = s.config.SimilarityThreshold
	}

	// Normalize the input text
	var normalizeInfo normalize.Result
	var normalizedText string

	if opts.IncludeDetails {
		normalizeInfo = normalize.NormalizeWithDetails(text)
		normalizedText = normalizeInfo.Normalized
	} else {
		normalizedText = normalize.Normalize(text)
	}

	// Generate embedding for the normalized text
	embedding, err := s.embeddingService.GetEmbedding(normalizedText)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	// Search for similar entities
	matchResults, err := s.qdrantClient.Search(ctx, embedding, opts.Limit, opts.Threshold)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}

	// Convert match results to matches
	matches := make([]Match, 0, len(matchResults))
	for _, result := range matchResults {
		match := Match{
			ID:           result.ID,
			OriginalText: result.OriginalText,
			Normalized:   result.Normalized,
			Score:        result.Score,
		}

		if opts.IncludeDetails {
			match.NormalizeInfo = &normalizeInfo
		}

		matches = append(matches, match)
	}

	return matches, nil
}

// AddEntity adds a new entity to the system
func (s *Service) AddEntity(ctx context.Context, entity EntityData) error {
	if strings.TrimSpace(entity.OriginalText) == "" {
		return fmt.Errorf("empty entity text")
	}

	// Generate ID if not provided
	if entity.ID == "" {
		entity.ID = fmt.Sprintf("entity_%d", time.Now().UnixNano())
	}

	// Normalize the text
	normalizedText := normalize.Normalize(entity.OriginalText)

	// Generate embedding
	embedding, err := s.embeddingService.GetEmbedding(normalizedText)
	if err != nil {
		return fmt.Errorf("failed to generate embedding: %w", err)
	}

	// Create entity record
	record := qdrant.EntityRecord{
		ID:           entity.ID,
		OriginalText: entity.OriginalText,
		Normalized:   normalizedText,
		Embedding:    embedding,
	}

	// Add to Qdrant
	if err := s.qdrantClient.Upsert(ctx, []qdrant.EntityRecord{record}); err != nil {
		return fmt.Errorf("failed to upsert entity: %w", err)
	}

	return nil
}

// AddEntities adds multiple entities in batch
func (s *Service) AddEntities(ctx context.Context, entities []EntityData) error {
	if len(entities) == 0 {
		return nil
	}

	// Prepare normalized texts for batch embedding
	normalizedTexts := make([]string, 0, len(entities))
	entityMap := make(map[int]EntityData)

	for _, entity := range entities {
		if strings.TrimSpace(entity.OriginalText) == "" {
			continue
		}

		normalizedText := normalize.Normalize(entity.OriginalText)
		normalizedTexts = append(normalizedTexts, normalizedText)
		entityMap[len(normalizedTexts)-1] = entity
	}

	// Generate embeddings in batch
	embeddings, err := s.embeddingService.GetEmbeddings(normalizedTexts)
	if err != nil {
		return fmt.Errorf("failed to generate embeddings: %w", err)
	}

	// Create entity records
	records := make([]qdrant.EntityRecord, 0, len(embeddings))

	for i, embedding := range embeddings {
		entity := entityMap[i]
		id := entity.ID

		// Generate ID if not provided
		if id == "" {
			id = fmt.Sprintf("entity_%d", time.Now().UnixNano()+int64(i))
		}

		record := qdrant.EntityRecord{
			ID:           id,
			OriginalText: entity.OriginalText,
			Normalized:   normalizedTexts[i],
			Embedding:    embedding,
		}

		records = append(records, record)
	}

	// Add to Qdrant
	if err := s.qdrantClient.Upsert(ctx, records); err != nil {
		return fmt.Errorf("failed to upsert entities: %w", err)
	}

	return nil
}

// DeleteEntity deletes an entity by ID
func (s *Service) DeleteEntity(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("empty entity ID")
	}

	return s.qdrantClient.Delete(ctx, []string{id})
}

// GetEntityCount returns the number of entities in the system
func (s *Service) GetEntityCount(ctx context.Context) (uint64, error) {
	return s.qdrantClient.GetCount(ctx)
}
