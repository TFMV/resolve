package embed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/TFMV/resolve/config"
)

// EmbeddingService provides an interface to generate embeddings for text
type EmbeddingService interface {
	// GetEmbeddings generates embeddings for multiple texts
	GetEmbeddings(texts []string) ([][]float32, error)

	// GetEmbedding generates an embedding for a single text
	GetEmbedding(text string) ([]float32, error)
}

// HTTPEmbeddingService communicates with a Python embedding service over HTTP
type HTTPEmbeddingService struct {
	client      *http.Client
	serviceURL  string
	servicePort int
}

// EmbeddingRequest represents the request to the embedding service
type EmbeddingRequest struct {
	Texts []string `json:"texts"`
}

// EmbeddingResponse represents the response from the embedding service
type EmbeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	Error      string      `json:"error,omitempty"`
}

// NewEmbeddingService creates a new embedding service
func NewEmbeddingService(cfg *config.Config) EmbeddingService {
	return &HTTPEmbeddingService{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		serviceURL:  cfg.EmbeddingServiceURL,
		servicePort: cfg.EmbeddingServicePort,
	}
}

// GetEmbeddings gets embeddings for multiple texts
func (s *HTTPEmbeddingService) GetEmbeddings(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Create request body
	reqBody := EmbeddingRequest{
		Texts: texts,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	// Create the HTTP request
	url := fmt.Sprintf("%s:%d/embed", s.serviceURL, s.servicePort)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send embedding request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding service returned status code %d", resp.StatusCode)
	}

	// Parse the response
	var embedResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("failed to decode embedding response: %w", err)
	}

	// Check if there was an error from the service
	if embedResp.Error != "" {
		return nil, fmt.Errorf("embedding service error: %s", embedResp.Error)
	}

	return embedResp.Embeddings, nil
}

// GetEmbedding gets an embedding for a single text
func (s *HTTPEmbeddingService) GetEmbedding(text string) ([]float32, error) {
	embeddings, err := s.GetEmbeddings([]string{text})
	if err != nil {
		return nil, err
	}

	if len(embeddings) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return embeddings[0], nil
}

// MockEmbeddingService provides a mock implementation for testing
type MockEmbeddingService struct {
	// VectorSize is the size of the mock embeddings to generate
	VectorSize int
}

// NewMockEmbeddingService creates a new mock embedding service
func NewMockEmbeddingService(vectorSize int) EmbeddingService {
	return &MockEmbeddingService{
		VectorSize: vectorSize,
	}
}

// GetEmbeddings returns mock embeddings (all 0.1) for testing
func (m *MockEmbeddingService) GetEmbeddings(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Generate mock embeddings - for testing only
	result := make([][]float32, len(texts))
	for i := range texts {
		embedding := make([]float32, m.VectorSize)
		for j := range embedding {
			embedding[j] = 0.1 // Mock value
		}
		result[i] = embedding
	}

	return result, nil
}

// GetEmbedding returns a mock embedding for a single text
func (m *MockEmbeddingService) GetEmbedding(text string) ([]float32, error) {
	if text == "" {
		return nil, fmt.Errorf("empty text")
	}

	// Generate mock embedding
	embedding := make([]float32, m.VectorSize)
	for i := range embedding {
		embedding[i] = 0.1 // Mock value
	}

	return embedding, nil
}
