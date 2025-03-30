package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/TFMV/resolve/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/qdrant"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Server represents the HTTP API server
type Server struct {
	router           *chi.Mux
	matchService     *match.Service
	config           *config.Config
	qdrantClient     *qdrant.Client
	embeddingService embed.EmbeddingService
}

// MatchRequest represents a match request
type MatchRequest struct {
	Text           string  `json:"text"`
	Limit          int     `json:"limit,omitempty"`
	Threshold      float32 `json:"threshold,omitempty"`
	IncludeDetails bool    `json:"include_details,omitempty"`
}

// IngestRequest represents an ingestion request
type IngestRequest struct {
	Entities []match.EntityData `json:"entities"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, qdrantClient *qdrant.Client, embeddingService embed.EmbeddingService) *Server {
	router := chi.NewRouter()

	// Create match service
	matchService := match.NewService(cfg, qdrantClient, embeddingService)

	server := &Server{
		router:           router,
		matchService:     matchService,
		config:           cfg,
		qdrantClient:     qdrantClient,
		embeddingService: embeddingService,
	}

	// Set up middleware
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(30 * time.Second))

	// Set up routes
	router.Get("/", server.handleIndex)
	router.Get("/health", server.handleHealth)

	router.Route("/api", func(r chi.Router) {
		r.Post("/match", server.handleMatch)
		r.Post("/ingest", server.handleIngest)
		r.Get("/stats", server.handleStats)
	})

	return server
}

// Start starts the HTTP server
func (s *Server) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting API server on %s", addr)
	return http.ListenAndServe(addr, s.router)
}

// handleIndex handles the root route
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"name":    "Resolve Entity Matching API",
		"version": "1.0.0",
	})
}

// handleHealth handles the health check route
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Check Qdrant health
	qdrantVersion, err := s.qdrantClient.Health(ctx)
	qdrantHealth := "ok"
	if err != nil {
		qdrantHealth = fmt.Sprintf("error: %v", err)
	}

	// Prepare response
	response := map[string]interface{}{
		"status": "ok",
		"components": map[string]string{
			"qdrant":         qdrantHealth,
			"qdrant_version": qdrantVersion,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleMatch handles entity matching requests
func (s *Server) handleMatch(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req MatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.Text == "" {
		s.sendError(w, "Text is required", http.StatusBadRequest)
		return
	}

	// Set default values
	if req.Limit <= 0 {
		req.Limit = 10
	}

	if req.Threshold <= 0 {
		req.Threshold = s.config.SimilarityThreshold
	}

	// Create options
	opts := match.Options{
		Limit:          req.Limit,
		Threshold:      req.Threshold,
		IncludeDetails: req.IncludeDetails,
	}

	// Execute search
	matches, err := s.matchService.FindMatches(r.Context(), req.Text, opts)
	if err != nil {
		s.sendError(w, fmt.Sprintf("Search failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

// handleIngest handles entity ingestion requests
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Entities) == 0 {
		s.sendError(w, "No entities provided", http.StatusBadRequest)
		return
	}

	// Ingest entities
	err := s.matchService.AddEntities(r.Context(), req.Entities)
	if err != nil {
		s.sendError(w, fmt.Sprintf("Ingestion failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"ingested": len(req.Entities),
	})
}

// handleStats handles stats requests
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Get entity count
	count, err := s.matchService.GetEntityCount(ctx)
	if err != nil {
		s.sendError(w, fmt.Sprintf("Failed to get stats: %v", err), http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entity_count": count,
	})
}

// sendError sends an error response
func (s *Server) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
	})
}
