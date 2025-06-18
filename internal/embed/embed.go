package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/TFMV/resolve/internal/config"
)

// EmbeddingService defines the interface for embedding services
type EmbeddingService interface {
	GetEmbedding(ctx context.Context, text string) ([]float32, error)
	GetEmbeddingBatch(ctx context.Context, texts []string) ([][]float32, error)
	Health(ctx context.Context) error
}

// HTTPClient represents the embedding service client
type HTTPClient struct {
	client       *http.Client
	url          string
	modelName    string
	embeddingDim int
	batchSize    int

	// Simple cache implementation
	cacheMutex sync.RWMutex
	cache      map[string][]float32
	cacheSize  int
}

// embeddingRequest represents the request to the embedding service
type embeddingRequest struct {
	Texts     []string `json:"texts"`
	ModelName string   `json:"model_name,omitempty"`
}

// embeddingResponse represents the response from the embedding service
type embeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	Error      string      `json:"error,omitempty"`
}

// NewHTTPClient creates a new embedding service client
func NewHTTPClient(cfg *config.Config) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{
			Timeout: time.Duration(cfg.Embedding.Timeout) * time.Second,
		},
		url:          cfg.Embedding.URL,
		modelName:    cfg.Embedding.ModelName,
		embeddingDim: cfg.Embedding.EmbeddingDim,
		batchSize:    cfg.Embedding.BatchSize,
		cache:        make(map[string][]float32, cfg.Embedding.CacheSize),
		cacheSize:    cfg.Embedding.CacheSize,
	}
}

// GetEmbedding gets an embedding for a single text
func (c *HTTPClient) GetEmbedding(ctx context.Context, text string) ([]float32, error) {
	if text == "" {
		return make([]float32, c.embeddingDim), nil
	}

	// Check cache first
	c.cacheMutex.RLock()
	emb, found := c.cache[text]
	c.cacheMutex.RUnlock()

	if found {
		return emb, nil
	}

	// Get embedding from service
	embeddings, err := c.GetEmbeddingBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}

	if len(embeddings) == 0 {
		return nil, errors.New("empty response from embedding service")
	}

	// Cache the result
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	// Simple eviction policy: if cache is full, just skip caching
	if len(c.cache) < c.cacheSize {
		c.cache[text] = embeddings[0]
	}

	return embeddings[0], nil
}

// GetEmbeddingBatch gets embeddings for multiple texts
func (c *HTTPClient) GetEmbeddingBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	batchSize := c.batchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	results := make([][]float32, len(texts))

	// First attempt to satisfy from cache
	missingTexts := make([]string, 0)
	missingIdx := make([]int, 0)

	c.cacheMutex.RLock()
	for i, t := range texts {
		if emb, ok := c.cache[t]; ok {
			results[i] = emb
		} else {
			missingTexts = append(missingTexts, t)
			missingIdx = append(missingIdx, i)
		}
	}
	c.cacheMutex.RUnlock()

	if len(missingTexts) == 0 {
		return results, nil
	}

	// Fetch missing embeddings in batches
	fetched := 0
	for fetched < len(missingTexts) {
		end := fetched + batchSize
		if end > len(missingTexts) {
			end = len(missingTexts)
		}

		req := embeddingRequest{
			Texts:     missingTexts[fetched:end],
			ModelName: c.modelName,
		}

		jsonData, err := json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url+"/embed", bytes.NewBuffer(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
		}

		var res embeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		if res.Error != "" {
			return nil, fmt.Errorf("embedding service error: %s", res.Error)
		}

		if len(res.Embeddings) != end-fetched {
			return nil, fmt.Errorf("unexpected embeddings count")
		}

		c.cacheMutex.Lock()
		for i := range res.Embeddings {
			idx := missingIdx[fetched+i]
			results[idx] = res.Embeddings[i]
			if len(c.cache) < c.cacheSize {
				c.cache[missingTexts[fetched+i]] = res.Embeddings[i]
			}
		}
		c.cacheMutex.Unlock()

		fetched = end
	}

	return results, nil
}

// Health checks if the embedding service is healthy
func (c *HTTPClient) Health(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, "GET", c.url+"/health", nil)
	if err != nil {
		return fmt.Errorf("failed to create health request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send health request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("health check failed: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// MockEmbeddingService is a mock implementation for testing
type MockEmbeddingService struct {
	Dimension int
}

// NewMockEmbeddingService creates a new mock embedding service
func NewMockEmbeddingService(dimension int) *MockEmbeddingService {
	return &MockEmbeddingService{
		Dimension: dimension,
	}
}

// GetEmbedding returns a mock embedding for a single text
func (m *MockEmbeddingService) GetEmbedding(ctx context.Context, text string) ([]float32, error) {
	// Create deterministic mock embeddings based on text content
	embedding := make([]float32, m.Dimension)
	for i := 0; i < m.Dimension && i < len(text); i++ {
		if i < len(text) {
			embedding[i] = float32(text[i%len(text)]) / 255.0
		}
	}
	return embedding, nil
}

// GetEmbeddingBatch returns mock embeddings for multiple texts
func (m *MockEmbeddingService) GetEmbeddingBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		emb, _ := m.GetEmbedding(ctx, text)
		result[i] = emb
	}
	return result, nil
}

// Health always returns success for the mock
func (m *MockEmbeddingService) Health(ctx context.Context) error {
	return nil
}
