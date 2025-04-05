package weaviate

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/resolve/internal/config"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

// Client represents the Weaviate client wrapper
type Client struct {
	client         *weaviate.Client
	cfg            *config.Config
	className      string
	embeddingDim   int
	schemaInitDone bool
}

// EntityRecord represents an entity to be stored in the vector database
type EntityRecord struct {
	ID                string                 `json:"id,omitempty"`
	Name              string                 `json:"name,omitempty"`
	NameNormalized    string                 `json:"name_normalized,omitempty"`
	Address           string                 `json:"address,omitempty"`
	AddressNormalized string                 `json:"address_normalized,omitempty"`
	City              string                 `json:"city,omitempty"`
	CityNormalized    string                 `json:"city_normalized,omitempty"`
	State             string                 `json:"state,omitempty"`
	StateNormalized   string                 `json:"state_normalized,omitempty"`
	Zip               string                 `json:"zip,omitempty"`
	ZipNormalized     string                 `json:"zip_normalized,omitempty"`
	Phone             string                 `json:"phone,omitempty"`
	PhoneNormalized   string                 `json:"phone_normalized,omitempty"`
	Email             string                 `json:"email,omitempty"`
	EmailNormalized   string                 `json:"email_normalized,omitempty"`
	CreatedAt         int64                  `json:"created_at,omitempty"`
	UpdatedAt         int64                  `json:"updated_at,omitempty"`
	Vector            []float32              `json:"vector,omitempty"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// MatchResult represents a match result with score and explanation
type MatchResult struct {
	EntityRecord *EntityRecord          `json:"entity"`
	Score        float64                `json:"score"`
	Distance     float64                `json:"distance"`
	MatchID      string                 `json:"match_id"`
	MatchedOn    []string               `json:"matched_on"`
	Explanation  string                 `json:"explanation"`
	FieldScores  map[string]float64     `json:"field_scores,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// NewClient creates a new Weaviate client wrapper
func NewClient(cfg *config.Config, embeddingDim int) (*Client, error) {
	// Create authentication config if API key is provided
	var authConfig *auth.ApiKey
	if cfg.Weaviate.APIKey != "" {
		authConfig = &auth.ApiKey{Value: cfg.Weaviate.APIKey}
	}

	// Create client configuration
	clientConfig := weaviate.Config{
		Host:       cfg.Weaviate.Host,
		Scheme:     cfg.Weaviate.Scheme,
		AuthConfig: authConfig,
	}

	// Initialize client
	client, err := weaviate.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Weaviate client: %w", err)
	}

	return &Client{
		client:       client,
		cfg:          cfg,
		className:    cfg.Weaviate.ClassName,
		embeddingDim: embeddingDim,
	}, nil
}

// Health checks the connection to Weaviate
func (c *Client) Health(ctx context.Context) (bool, error) {
	liveChecker := c.client.Misc().LiveChecker()
	result, err := liveChecker.Do(ctx)
	if err != nil {
		return false, fmt.Errorf("health check failed: %w", err)
	}
	return result, nil
}

// InitSchema initializes the schema for storing entities
func (c *Client) InitSchema(ctx context.Context) error {
	// Check if schema already exists
	exists, err := c.classExists(ctx, c.className)
	if err != nil {
		return fmt.Errorf("failed to check if class exists: %w", err)
	}

	if exists {
		c.schemaInitDone = true
		return nil
	}

	// Define class properties
	entityClass := &models.Class{
		Class:       c.className,
		Description: fmt.Sprintf("Entity class for Resolve entity matching, created at %s", time.Now().Format(time.RFC3339)),
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, Description: "Entity name"},
			{Name: "name_normalized", DataType: []string{"text"}, Description: "Normalized entity name"},
			{Name: "address", DataType: []string{"text"}, Description: "Entity address"},
			{Name: "address_normalized", DataType: []string{"text"}, Description: "Normalized entity address"},
			{Name: "city", DataType: []string{"text"}, Description: "Entity city"},
			{Name: "city_normalized", DataType: []string{"text"}, Description: "Normalized entity city"},
			{Name: "state", DataType: []string{"text"}, Description: "Entity state"},
			{Name: "state_normalized", DataType: []string{"text"}, Description: "Normalized entity state"},
			{Name: "zip", DataType: []string{"text"}, Description: "Entity ZIP code"},
			{Name: "zip_normalized", DataType: []string{"text"}, Description: "Normalized entity ZIP code"},
			{Name: "phone", DataType: []string{"text"}, Description: "Entity phone"},
			{Name: "phone_normalized", DataType: []string{"text"}, Description: "Normalized entity phone"},
			{Name: "email", DataType: []string{"text"}, Description: "Entity email"},
			{Name: "email_normalized", DataType: []string{"text"}, Description: "Normalized entity email"},
			{Name: "created_at", DataType: []string{"int"}, Description: "Creation timestamp"},
			{Name: "updated_at", DataType: []string{"int"}, Description: "Update timestamp"},
			{Name: "metadata", DataType: []string{"object"}, Description: "Additional metadata"},
		},
		Vectorizer: "none", // We'll provide our own vectors
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
	}

	// Create class
	err = c.client.Schema().ClassCreator().WithClass(entityClass).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	c.schemaInitDone = true
	return nil
}

// classExists checks if a class exists in the schema
func (c *Client) classExists(ctx context.Context, className string) (bool, error) {
	schema, err := c.client.Schema().Getter().Do(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get schema: %w", err)
	}

	for _, class := range schema.Classes {
		if class.Class == className {
			return true, nil
		}
	}

	return false, nil
}

// AddEntity adds a new entity to the vector database
func (c *Client) AddEntity(ctx context.Context, entity *EntityRecord) (string, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return "", err
		}
	}

	// Generate ID if not provided
	if entity.ID == "" {
		entity.ID = uuid.New().String()
	}

	// Set timestamps
	now := time.Now().Unix()
	if entity.CreatedAt == 0 {
		entity.CreatedAt = now
	}
	entity.UpdatedAt = now

	// Prepare object properties (excluding vector)
	objProperties := map[string]interface{}{
		"name":               entity.Name,
		"name_normalized":    entity.NameNormalized,
		"address":            entity.Address,
		"address_normalized": entity.AddressNormalized,
		"city":               entity.City,
		"city_normalized":    entity.CityNormalized,
		"state":              entity.State,
		"state_normalized":   entity.StateNormalized,
		"zip":                entity.Zip,
		"zip_normalized":     entity.ZipNormalized,
		"phone":              entity.Phone,
		"phone_normalized":   entity.PhoneNormalized,
		"email":              entity.Email,
		"email_normalized":   entity.EmailNormalized,
		"created_at":         entity.CreatedAt,
		"updated_at":         entity.UpdatedAt,
	}

	// Add metadata if provided
	if entity.Metadata != nil {
		objProperties["metadata"] = entity.Metadata
	}

	// Add object to Weaviate
	_, err := c.client.Data().Creator().
		WithID(entity.ID).
		WithClassName(c.className).
		WithProperties(objProperties).
		WithVector(entity.Vector).
		Do(ctx)

	if err != nil {
		return "", fmt.Errorf("failed to add entity: %w", err)
	}

	return entity.ID, nil
}

// BatchAddEntities adds multiple entities in a batch
func (c *Client) BatchAddEntities(ctx context.Context, entities []*EntityRecord) ([]string, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return nil, err
		}
	}

	batchSize := 100 // Weaviate recommends batches of 100-200 objects
	batcher := c.client.Batch().ObjectsBatcher()
	results := make([]string, len(entities))
	now := time.Now().Unix()

	for i, entity := range entities {
		// Generate ID if not provided
		if entity.ID == "" {
			entity.ID = uuid.New().String()
		}
		results[i] = entity.ID

		// Set timestamps
		if entity.CreatedAt == 0 {
			entity.CreatedAt = now
		}
		entity.UpdatedAt = now

		// Prepare object properties
		objProperties := map[string]interface{}{
			"name":               entity.Name,
			"name_normalized":    entity.NameNormalized,
			"address":            entity.Address,
			"address_normalized": entity.AddressNormalized,
			"city":               entity.City,
			"city_normalized":    entity.CityNormalized,
			"state":              entity.State,
			"state_normalized":   entity.StateNormalized,
			"zip":                entity.Zip,
			"zip_normalized":     entity.ZipNormalized,
			"phone":              entity.Phone,
			"phone_normalized":   entity.PhoneNormalized,
			"email":              entity.Email,
			"email_normalized":   entity.EmailNormalized,
			"created_at":         entity.CreatedAt,
			"updated_at":         entity.UpdatedAt,
		}

		// Add metadata if provided
		if entity.Metadata != nil {
			objProperties["metadata"] = entity.Metadata
		}

		// Add to batch
		batcher = batcher.WithObjects(&models.Object{
			Class:      c.className,
			ID:         strfmt.UUID(entity.ID),
			Properties: objProperties,
			Vector:     entity.Vector,
		})

		// Execute batch when it reaches the batch size
		if (i+1)%batchSize == 0 || i == len(entities)-1 {
			_, err := batcher.Do(ctx)
			if err != nil {
				return results[:i+1], fmt.Errorf("failed to execute batch: %w", err)
			}

			// Reset batcher for next batch
			batcher = c.client.Batch().ObjectsBatcher()
		}
	}

	return results, nil
}

// SearchEntities searches for entities by vector similarity
func (c *Client) SearchEntities(ctx context.Context, vector []float32, limit int, filterParams map[string]string) ([]*EntityRecord, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return nil, err
		}
	}

	// Build query
	nearVectorQuery := c.client.GraphQL().NearVectorArgBuilder().
		WithVector(vector)

	// Build filter if provided
	var where *filters.WhereBuilder
	if len(filterParams) > 0 {
		// Create a filter for each parameter
		var whereFilters []*filters.WhereBuilder
		for field, value := range filterParams {
			whereFilter := filters.Where().
				WithPath([]string{field}).
				WithOperator(filters.Equal).
				WithValueString(value)
			whereFilters = append(whereFilters, whereFilter)
		}

		// If multiple filters, combine them with AND
		if len(whereFilters) > 1 {
			where = filters.Where().
				WithOperator(filters.And).
				WithOperands(whereFilters)
		} else {
			where = whereFilters[0]
		}
	}

	// Build field selection
	fields := []graphql.Field{
		{Name: "name"},
		{Name: "name_normalized"},
		{Name: "address"},
		{Name: "address_normalized"},
		{Name: "city"},
		{Name: "city_normalized"},
		{Name: "state"},
		{Name: "state_normalized"},
		{Name: "zip"},
		{Name: "zip_normalized"},
		{Name: "phone"},
		{Name: "phone_normalized"},
		{Name: "email"},
		{Name: "email_normalized"},
		{Name: "created_at"},
		{Name: "updated_at"},
		{Name: "metadata"},
		{Name: "_additional", Fields: []graphql.Field{
			{Name: "id"},
			{Name: "distance"},
			{Name: "vector"},
		}},
	}

	// Execute search
	query := c.client.GraphQL().Get().
		WithClassName(c.className).
		WithFields(fields...).
		WithNearVector(nearVectorQuery).
		WithLimit(limit)

	// Add filter if provided
	if where != nil {
		query = query.WithWhere(where)
	}

	// Execute query
	result, err := query.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}

	// Parse results
	entities := make([]*EntityRecord, 0)
	if len(result.Data["Get"].(map[string]interface{})[c.className].([]interface{})) == 0 {
		return entities, nil
	}

	for _, obj := range result.Data["Get"].(map[string]interface{})[c.className].([]interface{}) {
		entity := c.parseEntityFromResult(obj.(map[string]interface{}))
		entities = append(entities, entity)
	}

	return entities, nil
}

// FindMatches finds entity matches based on a query entity with custom scoring and explanation
func (c *Client) FindMatches(ctx context.Context, queryEntity *EntityRecord, threshold float64, limit int) ([]*MatchResult, error) {
	// Search for similar entities using the vector similarity
	entities, err := c.SearchEntities(ctx, queryEntity.Vector, limit, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to search for similar entities: %w", err)
	}

	// Process and score the matches
	matches := make([]*MatchResult, 0, len(entities))
	for _, entity := range entities {
		// Calculate vector similarity score (convert cosine distance to similarity)
		vectorScore := 1.0
		fieldScores := make(map[string]float64)
		matchedFields := make([]string, 0)
		totalScore := 0.0
		distance := 0.0

		// Get the distance from the search results if available
		if entity.Metadata != nil {
			if distVal, ok := entity.Metadata["distance"].(float64); ok {
				distance = distVal
				vectorScore = 1.0 - distance
				totalScore = vectorScore
				fieldScores["vector"] = vectorScore
			}
		}

		// Check if the match exceeds the threshold
		if totalScore < threshold {
			continue
		}

		// Create detailed explanation
		explanation := fmt.Sprintf("Vector similarity score: %.2f", vectorScore)

		// Add to matched fields if score is high enough
		if vectorScore >= 0.75 {
			matchedFields = append(matchedFields, "vector")
		}

		// Create the match result
		match := &MatchResult{
			EntityRecord: entity,
			Score:        totalScore,
			Distance:     distance,
			MatchID:      entity.ID,
			MatchedOn:    matchedFields,
			Explanation:  explanation,
			FieldScores:  fieldScores,
		}

		matches = append(matches, match)
	}

	return matches, nil
}

// GetEntity retrieves an entity by ID
func (c *Client) GetEntity(ctx context.Context, id string) (*EntityRecord, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return nil, err
		}
	}

	// Execute get
	objects, err := c.client.Data().ObjectsGetter().
		WithID(id).
		WithClassName(c.className).
		WithVector().
		Do(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to get entity: %w", err)
	}

	if len(objects) == 0 {
		return nil, fmt.Errorf("entity not found with ID: %s", id)
	}

	// Use the first object from the result
	result := objects[0]

	// Convert to EntityRecord
	entity := &EntityRecord{
		ID:     id,
		Vector: result.Vector,
	}

	// Extract properties
	if props, ok := result.Properties.(map[string]interface{}); ok {
		if name, ok := props["name"].(string); ok {
			entity.Name = name
		}
		if normalizedName, ok := props["name_normalized"].(string); ok {
			entity.NameNormalized = normalizedName
		}
		if address, ok := props["address"].(string); ok {
			entity.Address = address
		}
		if normalizedAddress, ok := props["address_normalized"].(string); ok {
			entity.AddressNormalized = normalizedAddress
		}
		if city, ok := props["city"].(string); ok {
			entity.City = city
		}
		if normalizedCity, ok := props["city_normalized"].(string); ok {
			entity.CityNormalized = normalizedCity
		}
		if state, ok := props["state"].(string); ok {
			entity.State = state
		}
		if normalizedState, ok := props["state_normalized"].(string); ok {
			entity.StateNormalized = normalizedState
		}
		if zip, ok := props["zip"].(string); ok {
			entity.Zip = zip
		}
		if normalizedZip, ok := props["zip_normalized"].(string); ok {
			entity.ZipNormalized = normalizedZip
		}
		if phone, ok := props["phone"].(string); ok {
			entity.Phone = phone
		}
		if normalizedPhone, ok := props["phone_normalized"].(string); ok {
			entity.PhoneNormalized = normalizedPhone
		}
		if email, ok := props["email"].(string); ok {
			entity.Email = email
		}
		if normalizedEmail, ok := props["email_normalized"].(string); ok {
			entity.EmailNormalized = normalizedEmail
		}
		if createdAt, ok := props["created_at"].(int64); ok {
			entity.CreatedAt = createdAt
		} else if createdAt, ok := props["created_at"].(float64); ok {
			entity.CreatedAt = int64(createdAt)
		}
		if updatedAt, ok := props["updated_at"].(int64); ok {
			entity.UpdatedAt = updatedAt
		} else if updatedAt, ok := props["updated_at"].(float64); ok {
			entity.UpdatedAt = int64(updatedAt)
		}
		if metadata, ok := props["metadata"].(map[string]interface{}); ok {
			entity.Metadata = metadata
		}
	}

	return entity, nil
}

// UpdateEntity updates an existing entity
func (c *Client) UpdateEntity(ctx context.Context, entity *EntityRecord) error {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return err
		}
	}

	// Ensure ID is provided
	if entity.ID == "" {
		return fmt.Errorf("entity ID is required for updates")
	}

	// Set update timestamp
	entity.UpdatedAt = time.Now().Unix()

	// Prepare object properties
	objProperties := map[string]interface{}{
		"name":               entity.Name,
		"name_normalized":    entity.NameNormalized,
		"address":            entity.Address,
		"address_normalized": entity.AddressNormalized,
		"city":               entity.City,
		"city_normalized":    entity.CityNormalized,
		"state":              entity.State,
		"state_normalized":   entity.StateNormalized,
		"zip":                entity.Zip,
		"zip_normalized":     entity.ZipNormalized,
		"phone":              entity.Phone,
		"phone_normalized":   entity.PhoneNormalized,
		"email":              entity.Email,
		"email_normalized":   entity.EmailNormalized,
		"updated_at":         entity.UpdatedAt,
	}

	// Add metadata if provided
	if entity.Metadata != nil {
		objProperties["metadata"] = entity.Metadata
	}

	// Update object
	err := c.client.Data().Updater().
		WithID(entity.ID).
		WithClassName(c.className).
		WithProperties(objProperties).
		WithVector(entity.Vector).
		Do(ctx)

	if err != nil {
		return fmt.Errorf("failed to update entity: %w", err)
	}

	return nil
}

// DeleteEntity deletes an entity by ID
func (c *Client) DeleteEntity(ctx context.Context, id string) error {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return err
		}
	}

	// Delete object
	err := c.client.Data().Deleter().
		WithID(id).
		WithClassName(c.className).
		Do(ctx)

	if err != nil {
		return fmt.Errorf("failed to delete entity: %w", err)
	}

	return nil
}

// GetCount gets the total count of entities
func (c *Client) GetCount(ctx context.Context) (int, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return 0, err
		}
	}

	// Create aggregate query
	result, err := c.client.GraphQL().Aggregate().
		WithClassName(c.className).
		WithFields(
			graphql.Field{
				Name: "meta",
				Fields: []graphql.Field{
					{Name: "count"},
				},
			},
		).
		Do(ctx)

	if err != nil {
		return 0, fmt.Errorf("failed to get count: %w", err)
	}

	// Extract count
	if agg, ok := result.Data["Aggregate"].(map[string]interface{}); ok {
		if className, ok := agg[c.className].([]interface{}); ok && len(className) > 0 {
			if meta, ok := className[0].(map[string]interface{})["meta"].(map[string]interface{}); ok {
				if count, ok := meta["count"].(float64); ok {
					return int(count), nil
				}
			}
		}
	}

	return 0, fmt.Errorf("failed to parse count from response")
}

// parseEntityFromResult converts a GraphQL result into an EntityRecord
func (c *Client) parseEntityFromResult(obj map[string]interface{}) *EntityRecord {
	entity := &EntityRecord{}

	// Extract additional properties
	if additional, ok := obj["_additional"].(map[string]interface{}); ok {
		if id, ok := additional["id"].(string); ok {
			entity.ID = id
		}
		if vector, ok := additional["vector"].([]interface{}); ok {
			entity.Vector = make([]float32, len(vector))
			for i, v := range vector {
				if f, ok := v.(float64); ok {
					entity.Vector[i] = float32(f)
				}
			}
		}
		// Store distance in metadata for later use in scoring
		if distance, ok := additional["distance"].(float64); ok {
			if entity.Metadata == nil {
				entity.Metadata = make(map[string]interface{})
			}
			entity.Metadata["distance"] = distance
		}
	}

	// Extract standard properties
	if name, ok := obj["name"].(string); ok {
		entity.Name = name
	}
	if normalizedName, ok := obj["name_normalized"].(string); ok {
		entity.NameNormalized = normalizedName
	}
	if address, ok := obj["address"].(string); ok {
		entity.Address = address
	}
	if normalizedAddress, ok := obj["address_normalized"].(string); ok {
		entity.AddressNormalized = normalizedAddress
	}
	if city, ok := obj["city"].(string); ok {
		entity.City = city
	}
	if normalizedCity, ok := obj["city_normalized"].(string); ok {
		entity.CityNormalized = normalizedCity
	}
	if state, ok := obj["state"].(string); ok {
		entity.State = state
	}
	if normalizedState, ok := obj["state_normalized"].(string); ok {
		entity.StateNormalized = normalizedState
	}
	if zip, ok := obj["zip"].(string); ok {
		entity.Zip = zip
	}
	if normalizedZip, ok := obj["zip_normalized"].(string); ok {
		entity.ZipNormalized = normalizedZip
	}
	if phone, ok := obj["phone"].(string); ok {
		entity.Phone = phone
	}
	if normalizedPhone, ok := obj["phone_normalized"].(string); ok {
		entity.PhoneNormalized = normalizedPhone
	}
	if email, ok := obj["email"].(string); ok {
		entity.Email = email
	}
	if normalizedEmail, ok := obj["email_normalized"].(string); ok {
		entity.EmailNormalized = normalizedEmail
	}
	if createdAt, ok := obj["created_at"].(float64); ok {
		entity.CreatedAt = int64(createdAt)
	}
	if updatedAt, ok := obj["updated_at"].(float64); ok {
		entity.UpdatedAt = int64(updatedAt)
	}
	if metadata, ok := obj["metadata"].(map[string]interface{}); ok {
		// Merge with any existing metadata (like distance that we might have already added)
		if entity.Metadata == nil {
			entity.Metadata = metadata
		} else {
			for k, v := range metadata {
				entity.Metadata[k] = v
			}
		}
	}

	return entity
}

// ListEntities retrieves a paginated list of entities from Weaviate
func (c *Client) ListEntities(ctx context.Context, offset int, limit int) ([]*EntityRecord, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return nil, err
		}
	}

	// Build field selection
	fields := []graphql.Field{
		{Name: "name"},
		{Name: "name_normalized"},
		{Name: "address"},
		{Name: "address_normalized"},
		{Name: "city"},
		{Name: "city_normalized"},
		{Name: "state"},
		{Name: "state_normalized"},
		{Name: "zip"},
		{Name: "zip_normalized"},
		{Name: "phone"},
		{Name: "phone_normalized"},
		{Name: "email"},
		{Name: "email_normalized"},
		{Name: "created_at"},
		{Name: "updated_at"},
		{Name: "metadata"},
		{Name: "_additional", Fields: []graphql.Field{
			{Name: "id"},
			{Name: "vector"},
		}},
	}

	// Execute query
	query := c.client.GraphQL().Get().
		WithClassName(c.className).
		WithFields(fields...).
		WithLimit(limit).
		WithOffset(offset)

	// Execute query
	result, err := query.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Parse results
	entities := make([]*EntityRecord, 0)
	if len(result.Data["Get"].(map[string]interface{})[c.className].([]interface{})) == 0 {
		return entities, nil
	}

	for _, obj := range result.Data["Get"].(map[string]interface{})[c.className].([]interface{}) {
		entity := c.parseEntityFromResult(obj.(map[string]interface{}))
		entities = append(entities, entity)
	}

	return entities, nil
}

// BatchUpdateEntities updates multiple entities in a batch
func (c *Client) BatchUpdateEntities(ctx context.Context, entities []*EntityRecord) ([]string, error) {
	if !c.schemaInitDone {
		if err := c.InitSchema(ctx); err != nil {
			return nil, err
		}
	}

	batchSize := 100 // Weaviate recommends batches of 100-200 objects
	batcher := c.client.Batch().ObjectsBatcher()
	results := make([]string, len(entities))
	now := time.Now().Unix()

	for i, entity := range entities {
		// Generate ID if not provided
		if entity.ID == "" {
			entity.ID = uuid.New().String()
		}
		results[i] = entity.ID

		// Update timestamp
		entity.UpdatedAt = now

		// Prepare object properties
		objProperties := map[string]interface{}{
			"name":               entity.Name,
			"name_normalized":    entity.NameNormalized,
			"address":            entity.Address,
			"address_normalized": entity.AddressNormalized,
			"city":               entity.City,
			"city_normalized":    entity.CityNormalized,
			"state":              entity.State,
			"state_normalized":   entity.StateNormalized,
			"zip":                entity.Zip,
			"zip_normalized":     entity.ZipNormalized,
			"phone":              entity.Phone,
			"phone_normalized":   entity.PhoneNormalized,
			"email":              entity.Email,
			"email_normalized":   entity.EmailNormalized,
			"created_at":         entity.CreatedAt,
			"updated_at":         entity.UpdatedAt,
		}

		// Add metadata if provided
		if entity.Metadata != nil {
			objProperties["metadata"] = entity.Metadata
		}

		// Add to batch
		batcher = batcher.WithObjects(&models.Object{
			Class:      c.className,
			ID:         strfmt.UUID(entity.ID),
			Properties: objProperties,
			Vector:     entity.Vector,
		})

		// Execute batch when it reaches the batch size
		if (i+1)%batchSize == 0 || i == len(entities)-1 {
			_, err := batcher.Do(ctx)
			if err != nil {
				return results[:i+1], fmt.Errorf("failed to execute batch update: %w", err)
			}

			// Reset batcher for next batch
			batcher = c.client.Batch().ObjectsBatcher()
		}
	}

	return results, nil
}
