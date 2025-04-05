package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/weaviate"
	"github.com/gorilla/mux"
)

// Time format constant
const timeFormat = time.RFC3339

// timeNow returns the current time
var timeNow = time.Now

// MatchRequest represents a request to match an entity
type MatchRequest struct {
	Entity            *weaviate.EntityRecord `json:"entity"`
	Text              string                 `json:"text,omitempty"`
	Threshold         float64                `json:"threshold"`
	Limit             int                    `json:"limit"`
	UseCluster        bool                   `json:"use_clustering,omitempty"`
	IncludeScores     bool                   `json:"include_scores,omitempty"`
	FieldWeights      map[string]float32     `json:"field_weights,omitempty"`
	FieldTypeMappings map[string]string      `json:"field_type_mappings,omitempty"`
}

// MatchGroupRequest represents a request to retrieve a match group
type MatchGroupRequest struct {
	ThresholdOverride float32            `json:"threshold_override,omitempty"`
	MaxSize           int                `json:"max_size,omitempty"`
	IncludeScores     bool               `json:"include_scores,omitempty"`
	Strategy          string             `json:"strategy,omitempty"` // "direct", "transitive", or "hybrid"
	HopsLimit         int                `json:"hops_limit,omitempty"`
	FieldWeights      map[string]float32 `json:"field_weights,omitempty"`
}

// Server represents the API server
type Server struct {
	router       *mux.Router
	config       *config.Config
	vdbClient    *weaviate.Client
	matchService *match.Service
	httpServer   *http.Server
	embeddingDim int
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, vdbClient *weaviate.Client, matchService *match.Service, embeddingDim int) *Server {
	return &Server{
		config:       cfg,
		vdbClient:    vdbClient,
		matchService: matchService,
		embeddingDim: embeddingDim,
		router:       mux.NewRouter(),
	}
}

// registerRoutes registers all API routes
func (s *Server) registerRoutes() {
	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods(http.MethodGet)

	// Entity endpoints
	s.router.HandleFunc("/entities", s.handleAddEntity).Methods(http.MethodPost)
	s.router.HandleFunc("/entities/{id}", s.handleGetEntity).Methods(http.MethodGet)
	s.router.HandleFunc("/entities/{id}", s.handleUpdateEntity).Methods(http.MethodPut)
	s.router.HandleFunc("/entities/{id}", s.handleDeleteEntity).Methods(http.MethodDelete)
	s.router.HandleFunc("/entities/batch", s.handleBatchAddEntities).Methods(http.MethodPost)
	s.router.HandleFunc("/entities/count", s.handleGetEntityCount).Methods(http.MethodGet)

	// Matching endpoints
	s.router.HandleFunc("/match", s.handleMatchEntity).Methods(http.MethodPost)
	s.router.HandleFunc("/match/text", s.handleMatchText).Methods(http.MethodPost)

	// Match group endpoints
	s.router.HandleFunc("/entities/{id}/group", s.handleGetMatchGroup).Methods(http.MethodGet)
	s.router.HandleFunc("/entities/{id}/group", s.handleMatchGroupWithOptions).Methods(http.MethodPost)

	// Clustering endpoints
	s.router.HandleFunc("/clusters/recompute", s.handleRecomputeClusters).Methods(http.MethodPost)
}

// Start starts the API server
func (s *Server) Start() error {
	// Register routes
	s.registerRoutes()

	// Create the HTTP server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port),
		Handler:      s.router,
		ReadTimeout:  time.Duration(s.config.API.ReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(s.config.API.WriteTimeoutSecs) * time.Second,
		IdleTimeout:  time.Duration(s.config.API.IdleTimeoutSecs) * time.Second,
	}

	// Start the server
	log.Printf("Starting API server on %s:%d", s.config.API.Host, s.config.API.Port)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the API server
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// Health check handler
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Check vector database health
	vdbHealth, err := s.vdbClient.Health(r.Context())
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Vector database health check failed: "+err.Error())
		return
	}

	// Return health status
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"vdb_healthy": vdbHealth,
		"timestamp":   timeNow().Format(timeFormat),
	})
}

// Entity handlers

// handleGetEntities handles GET /entities
func (s *Server) handleGetEntities(w http.ResponseWriter, r *http.Request) {
	// Not implemented yet - will require pagination and possibly filtering
	respondWithError(w, http.StatusNotImplemented, "Get all entities is not implemented")
}

// handleAddEntity handles POST /entities
func (s *Server) handleAddEntity(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var entity weaviate.EntityRecord
	if err := json.NewDecoder(r.Body).Decode(&entity); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Check if vector is provided
	if len(entity.Vector) == 0 {
		respondWithError(w, http.StatusBadRequest, "Entity vector is required")
		return
	}

	// Check vector dimension
	if len(entity.Vector) != s.embeddingDim {
		respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid vector dimension: expected %d, got %d", s.embeddingDim, len(entity.Vector)))
		return
	}

	// Add entity
	id, err := s.vdbClient.AddEntity(r.Context(), &entity)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to add entity: "+err.Error())
		return
	}

	// Return success response
	respondWithJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// handleGetEntity handles GET /entities/{id}
func (s *Server) handleGetEntity(w http.ResponseWriter, r *http.Request) {
	// Get ID from path
	vars := mux.Vars(r)
	id := vars["id"]

	// Get entity
	entity, err := s.vdbClient.GetEntity(r.Context(), id)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Entity not found: "+err.Error())
		return
	}

	// Return entity
	respondWithJSON(w, http.StatusOK, entity)
}

// handleUpdateEntity handles PUT /entities/{id}
func (s *Server) handleUpdateEntity(w http.ResponseWriter, r *http.Request) {
	// Get ID from path
	vars := mux.Vars(r)
	id := vars["id"]

	// Parse request
	var entity weaviate.EntityRecord
	if err := json.NewDecoder(r.Body).Decode(&entity); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Set ID from path
	entity.ID = id

	// Check vector dimension if provided
	if len(entity.Vector) > 0 && len(entity.Vector) != s.embeddingDim {
		respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid vector dimension: expected %d, got %d", s.embeddingDim, len(entity.Vector)))
		return
	}

	// Update entity
	err := s.vdbClient.UpdateEntity(r.Context(), &entity)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to update entity: "+err.Error())
		return
	}

	// Return success response
	respondWithJSON(w, http.StatusOK, map[string]string{"status": "updated", "id": id})
}

// handleDeleteEntity handles DELETE /entities/{id}
func (s *Server) handleDeleteEntity(w http.ResponseWriter, r *http.Request) {
	// Get ID from path
	vars := mux.Vars(r)
	id := vars["id"]

	// Delete entity
	err := s.vdbClient.DeleteEntity(r.Context(), id)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to delete entity: "+err.Error())
		return
	}

	// Return success response
	respondWithJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

// handleBatchAddEntities handles POST /entities/batch
func (s *Server) handleBatchAddEntities(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var request struct {
		Entities []*weaviate.EntityRecord `json:"entities"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Check if entities are provided
	if len(request.Entities) == 0 {
		respondWithError(w, http.StatusBadRequest, "No entities provided")
		return
	}

	// Check vector dimensions
	for i, entity := range request.Entities {
		if len(entity.Vector) == 0 {
			respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Entity at index %d has no vector", i))
			return
		}
		if len(entity.Vector) != s.embeddingDim {
			respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Entity at index %d has invalid vector dimension: expected %d, got %d", i, s.embeddingDim, len(entity.Vector)))
			return
		}
	}

	// Add entities in batch
	ids, err := s.vdbClient.BatchAddEntities(r.Context(), request.Entities)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to add entities in batch: "+err.Error())
		return
	}

	// Return success response
	respondWithJSON(w, http.StatusCreated, map[string]interface{}{
		"status": "added",
		"count":  len(ids),
		"ids":    ids,
	})
}

// handleGetEntityCount handles GET /entities/count
func (s *Server) handleGetEntityCount(w http.ResponseWriter, r *http.Request) {
	// Get count
	count, err := s.vdbClient.GetCount(r.Context())
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get entity count: "+err.Error())
		return
	}

	// Return count
	respondWithJSON(w, http.StatusOK, map[string]int{"count": count})
}

// Matching handlers

// handleMatchEntity handles POST /match
func (s *Server) handleMatchEntity(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var request MatchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Set defaults
	if request.Threshold <= 0 {
		request.Threshold = float64(s.config.Matching.SimilarityThreshold)
	}
	if request.Limit <= 0 {
		request.Limit = s.config.Matching.DefaultLimit
	}

	// Check if entity and vector are provided
	if request.Entity == nil {
		respondWithError(w, http.StatusBadRequest, "Entity is required")
		return
	}
	if len(request.Entity.Vector) == 0 {
		respondWithError(w, http.StatusBadRequest, "Entity vector is required")
		return
	}

	// Check vector dimension
	if len(request.Entity.Vector) != s.embeddingDim {
		respondWithError(w, http.StatusBadRequest, fmt.Sprintf("Invalid vector dimension: expected %d, got %d", s.embeddingDim, len(request.Entity.Vector)))
		return
	}

	// Convert entity to match format
	entityData := match.EntityData{
		ID: request.Entity.ID,
		Fields: map[string]string{
			"name":    request.Entity.Name,
			"address": request.Entity.Address,
			"city":    request.Entity.City,
			"state":   request.Entity.State,
			"zip":     request.Entity.Zip,
			"phone":   request.Entity.Phone,
			"email":   request.Entity.Email,
		},
		Metadata: request.Entity.Metadata,
	}

	// Add normalized fields if available
	if request.Entity.NameNormalized != "" {
		entityData.Fields["name_normalized"] = request.Entity.NameNormalized
	}
	if request.Entity.AddressNormalized != "" {
		entityData.Fields["address_normalized"] = request.Entity.AddressNormalized
	}
	if request.Entity.CityNormalized != "" {
		entityData.Fields["city_normalized"] = request.Entity.CityNormalized
	}
	if request.Entity.StateNormalized != "" {
		entityData.Fields["state_normalized"] = request.Entity.StateNormalized
	}
	if request.Entity.ZipNormalized != "" {
		entityData.Fields["zip_normalized"] = request.Entity.ZipNormalized
	}
	if request.Entity.PhoneNormalized != "" {
		entityData.Fields["phone_normalized"] = request.Entity.PhoneNormalized
	}
	if request.Entity.EmailNormalized != "" {
		entityData.Fields["email_normalized"] = request.Entity.EmailNormalized
	}

	// Create match options
	matchOpts := match.Options{
		Limit:              request.Limit,
		Threshold:          float32(request.Threshold),
		IncludeDetails:     true,
		UseClustering:      request.UseCluster,
		IncludeFieldScores: request.IncludeScores,
		FieldWeights:       request.FieldWeights,
		FieldTypeMappings:  request.FieldTypeMappings,
	}

	// Find matches
	matches, err := s.matchService.FindMatchesForEntity(r.Context(), entityData, matchOpts)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to find matches: "+err.Error())
		return
	}

	// Return matches
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"matches": matches,
		"count":   len(matches),
	})
}

// handleMatchText handles POST /match/text
func (s *Server) handleMatchText(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var request struct {
		Text              string             `json:"text"`
		Threshold         float64            `json:"threshold"`
		Limit             int                `json:"limit"`
		UseCluster        bool               `json:"use_clustering,omitempty"`
		IncludeScores     bool               `json:"include_scores,omitempty"`
		FieldWeights      map[string]float32 `json:"field_weights,omitempty"`
		FieldTypeMappings map[string]string  `json:"field_type_mappings,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Check if text is provided
	if request.Text == "" {
		respondWithError(w, http.StatusBadRequest, "Text is required")
		return
	}

	// Set defaults
	if request.Threshold <= 0 {
		request.Threshold = float64(s.config.Matching.SimilarityThreshold)
	}
	if request.Limit <= 0 {
		request.Limit = s.config.Matching.DefaultLimit
	}

	// Create match options
	matchOpts := match.Options{
		Limit:              request.Limit,
		Threshold:          float32(request.Threshold),
		IncludeDetails:     true,
		UseClustering:      request.UseCluster,
		IncludeFieldScores: request.IncludeScores,
		FieldWeights:       request.FieldWeights,
		FieldTypeMappings:  request.FieldTypeMappings,
	}

	// Find matches
	matches, err := s.matchService.FindMatches(r.Context(), request.Text, matchOpts)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to find matches: "+err.Error())
		return
	}

	// Return matches
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"matches": matches,
		"count":   len(matches),
	})
}

// handleGetMatchGroup handles GET /entities/{id}/group
func (s *Server) handleGetMatchGroup(w http.ResponseWriter, r *http.Request) {
	// Get entity ID from path
	vars := mux.Vars(r)
	id := vars["id"]

	// Parse query parameters
	queryParams := r.URL.Query()

	// Extract threshold
	var threshold float32
	if thresholdStr := queryParams.Get("threshold"); thresholdStr != "" {
		thresholdVal, err := strconv.ParseFloat(thresholdStr, 32)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid threshold parameter")
			return
		}
		threshold = float32(thresholdVal)
	}

	// Extract max size
	var maxSize int
	if maxSizeStr := queryParams.Get("max_size"); maxSizeStr != "" {
		var err error
		maxSize, err = strconv.Atoi(maxSizeStr)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid max_size parameter")
			return
		}
	}

	// Extract include_scores
	includeScores := false
	if includeScoresStr := queryParams.Get("include_scores"); includeScoresStr == "true" {
		includeScores = true
	}

	// Extract strategy
	strategy := queryParams.Get("strategy")
	if strategy == "" {
		strategy = "hybrid" // Default strategy
	}
	if strategy != "direct" && strategy != "transitive" && strategy != "hybrid" {
		respondWithError(w, http.StatusBadRequest, "Invalid strategy parameter: must be 'direct', 'transitive', or 'hybrid'")
		return
	}

	// Extract hops limit
	var hopsLimit int
	if hopsLimitStr := queryParams.Get("hops_limit"); hopsLimitStr != "" {
		var err error
		hopsLimit, err = strconv.Atoi(hopsLimitStr)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid hops_limit parameter")
			return
		}
	}

	// Create options
	opts := match.MatchGroupOptions{
		ThresholdOverride: threshold,
		MaxGroupSize:      maxSize,
		IncludeScores:     includeScores,
		Strategy:          strategy,
		HopsLimit:         hopsLimit,
	}

	// Get match group
	group, err := s.matchService.GetMatchGroup(r.Context(), id, opts)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get match group: "+err.Error())
		return
	}

	// Return match group
	respondWithJSON(w, http.StatusOK, group)
}

// handleMatchGroupWithOptions handles POST /entities/{id}/group
func (s *Server) handleMatchGroupWithOptions(w http.ResponseWriter, r *http.Request) {
	// Get entity ID from path
	vars := mux.Vars(r)
	id := vars["id"]

	// Parse request
	var request MatchGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload: "+err.Error())
		return
	}

	// Create options
	opts := match.MatchGroupOptions{
		ThresholdOverride: request.ThresholdOverride,
		MaxGroupSize:      request.MaxSize,
		IncludeScores:     request.IncludeScores,
		Strategy:          request.Strategy,
		HopsLimit:         request.HopsLimit,
		FieldWeights:      request.FieldWeights,
	}

	// Apply defaults
	if opts.Strategy == "" {
		opts.Strategy = "hybrid" // Default strategy
	}

	// Get match group
	group, err := s.matchService.GetMatchGroup(r.Context(), id, opts)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to get match group: "+err.Error())
		return
	}

	// Return match group
	respondWithJSON(w, http.StatusOK, group)
}

// handleRecomputeClusters handles POST /clusters/recompute
func (s *Server) handleRecomputeClusters(w http.ResponseWriter, r *http.Request) {
	// Validate if the service supports recompute
	if s.matchService == nil || s.config == nil || !s.config.Clustering.Enabled {
		respondWithError(w, http.StatusBadRequest, "Clustering is not enabled in the current configuration")
		return
	}

	// Start recomputing clusters in a goroutine
	go func() {
		// TODO: Implement recompute functionality in match service
		log.Printf("Started cluster recomputation in background")

		// This would be implemented in the match service
		// err := s.matchService.RecomputeClusters(context.Background())
		// if err != nil {
		//    log.Printf("Error recomputing clusters: %v", err)
		// } else {
		//    log.Printf("Successfully recomputed clusters for all entities")
		// }
	}()

	// Return immediately with 202 Accepted
	respondWithJSON(w, http.StatusAccepted, map[string]string{
		"status":  "processing",
		"message": "Cluster recomputation started. This operation runs in the background and may take some time to complete.",
	})
}

// Response helpers

// respondWithError responds with an error
func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

// respondWithJSON responds with JSON
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "Failed to marshal response"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
