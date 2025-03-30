package qdrant

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/TFMV/resolve/config"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the Qdrant client to provide entity-specific operations
type Client struct {
	client         *qdrant.Client
	collectionName string
	vectorSize     uint64
}

// EntityRecord represents an entity stored in Qdrant
type EntityRecord struct {
	ID           string    `json:"id"`
	OriginalText string    `json:"original_text"`
	Normalized   string    `json:"normalized"`
	Embedding    []float32 `json:"-"` // Not stored in JSON
}

// MatchResult represents a match result from Qdrant
type MatchResult struct {
	ID           string  `json:"id"`
	OriginalText string  `json:"original_text"`
	Normalized   string  `json:"normalized"`
	Score        float32 `json:"score"`
}

// NewClient creates a new Qdrant client
func NewClient(cfg *config.Config) (*Client, error) {
	// Setup connection options
	var opts []grpc.DialOption

	if cfg.QdrantUseTLS {
		tlsConfig := &tls.Config{}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Create client
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:        cfg.QdrantHost,
		Port:        cfg.QdrantPort,
		APIKey:      cfg.QdrantAPIKey,
		UseTLS:      cfg.QdrantUseTLS,
		GrpcOptions: opts,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	return &Client{
		client:         client,
		collectionName: cfg.CollectionName,
		vectorSize:     cfg.VectorSize,
	}, nil
}

// Close closes the client connection
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// Health checks if Qdrant is available
func (c *Client) Health(ctx context.Context) (string, error) {
	result, err := c.client.HealthCheck(ctx)
	if err != nil {
		return "", err
	}

	return result.GetVersion(), nil
}

// CreateCollection creates a new collection if it doesn't exist
func (c *Client) CreateCollection(ctx context.Context) error {
	// Check if collection already exists
	collections, err := c.client.ListCollections(ctx)
	if err != nil {
		return err
	}

	for _, collection := range collections {
		if collection == c.collectionName {
			return nil // Collection already exists
		}
	}

	// Create the collection with default segment number
	defaultSegmentNumber := uint64(2)

	// Create the collection
	err = c.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: c.collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     c.vectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
		OptimizersConfig: &qdrant.OptimizersConfigDiff{
			DefaultSegmentNumber: &defaultSegmentNumber,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	return nil
}

// DeleteCollection deletes a collection
func (c *Client) DeleteCollection(ctx context.Context) error {
	err := c.client.DeleteCollection(ctx, c.collectionName)
	if err != nil {
		return fmt.Errorf("failed to delete collection: %w", err)
	}

	return nil
}

// Upsert adds or updates entity records in the collection
func (c *Client) Upsert(ctx context.Context, records []EntityRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Create points
	points := make([]*qdrant.PointStruct, 0, len(records))

	for _, record := range records {
		// Create metadata payload
		payload := map[string]any{
			"original_text": record.OriginalText,
			"normalized":    record.Normalized,
		}

		// Create point
		point := &qdrant.PointStruct{
			Id:      qdrant.NewID(record.ID),
			Vectors: qdrant.NewVectors(record.Embedding...),
			Payload: qdrant.NewValueMap(payload),
		}

		points = append(points, point)
	}

	// Set wait flag to ensure operation completes
	wait := true

	// Execute upsert
	_, err := c.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: c.collectionName,
		Points:         points,
		Wait:           &wait,
	})

	if err != nil {
		return fmt.Errorf("failed to upsert points: %w", err)
	}

	return nil
}

// Search finds entities similar to the query embedding
func (c *Client) Search(ctx context.Context, embedding []float32, limit int, threshold float32) ([]MatchResult, error) {
	if len(embedding) == 0 {
		return nil, fmt.Errorf("empty embedding")
	}

	// Create limit pointer
	limitUint64 := uint64(limit)

	// Set up search parameters
	params := &qdrant.QueryPoints{
		CollectionName: c.collectionName,
		Query:          qdrant.NewQuery(embedding...),
		Limit:          &limitUint64,
		WithPayload:    qdrant.NewWithPayload(true),
		ScoreThreshold: &threshold,
	}

	// Execute search
	results, err := c.client.Query(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}

	// Convert results
	matches := make([]MatchResult, 0, len(results))

	for _, point := range results {
		payload := point.GetPayload()

		// Get fields from payload
		originalText := ""
		normalizedText := ""

		if originalTextVal, ok := payload["original_text"]; ok {
			originalText = originalTextVal.GetStringValue()
		}

		if normalizedVal, ok := payload["normalized"]; ok {
			normalizedText = normalizedVal.GetStringValue()
		}

		// Create match result
		match := MatchResult{
			ID:           point.GetId().GetUuid(),
			OriginalText: originalText,
			Normalized:   normalizedText,
			Score:        point.GetScore(),
		}

		matches = append(matches, match)
	}

	return matches, nil
}

// GetCount returns the number of entities in the collection
func (c *Client) GetCount(ctx context.Context) (uint64, error) {
	count, err := c.client.Count(ctx, &qdrant.CountPoints{
		CollectionName: c.collectionName,
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get collection count: %w", err)
	}

	return count, nil
}

// Delete removes entities by their IDs
func (c *Client) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	// Convert string IDs to PointID
	pointIDs := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		pointIDs = append(pointIDs, qdrant.NewID(id))
	}

	// Set wait flag
	wait := true

	// Execute delete
	_, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: c.collectionName,
		Points:         qdrant.NewPointsSelectorIDs(pointIDs),
		Wait:           &wait,
	})

	if err != nil {
		return fmt.Errorf("failed to delete points: %w", err)
	}

	return nil
}

// GetByID retrieves an entity by its ID
func (c *Client) GetByID(ctx context.Context, id string) (*EntityRecord, error) {
	// Execute get
	results, err := c.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: c.collectionName,
		Ids:            []*qdrant.PointId{qdrant.NewID(id)},
		WithPayload:    qdrant.NewWithPayload(true),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get point: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("entity not found: %s", id)
	}

	// Get the first (and only) result
	point := results[0]
	payload := point.GetPayload()

	// Extract fields
	originalText := ""
	normalizedText := ""

	if originalTextVal, ok := payload["original_text"]; ok {
		originalText = originalTextVal.GetStringValue()
	}

	if normalizedVal, ok := payload["normalized"]; ok {
		normalizedText = normalizedVal.GetStringValue()
	}

	// Create entity record
	record := &EntityRecord{
		ID:           id,
		OriginalText: originalText,
		Normalized:   normalizedText,
	}

	return record, nil
}

// apiKeyCredentials implements the PerRPCCredentials interface for API key auth
type apiKeyCredentials struct {
	apiKey string
	secure bool
}

func newAPIKeyCredentials(apiKey string, secure bool) *apiKeyCredentials {
	return &apiKeyCredentials{
		apiKey: apiKey,
		secure: secure,
	}
}

func (c *apiKeyCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"api-key": c.apiKey,
	}, nil
}

func (c *apiKeyCredentials) RequireTransportSecurity() bool {
	return c.secure
}
