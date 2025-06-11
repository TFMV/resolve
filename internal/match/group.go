package match

import (
	"context"
	"fmt"
	"sort"

	"github.com/TFMV/resolve/internal/weaviate"
)

// MatchGroup represents a group of matching entities
type MatchGroup struct {
	ID           string        `json:"id"`         // Group identifier (derived from primary entity ID)
	Entities     []MatchResult `json:"entities"`   // All entities in the group
	Score        float32       `json:"score"`      // Average match score within the group
	Size         int           `json:"size"`       // Number of entities in the group
	PrimaryID    string        `json:"primary_id"` // ID of the primary/canonical entity
	SampleFields map[string]struct {
		Value      string  `json:"value"`      // Sample value for the field
		Agreement  float32 `json:"agreement"`  // Percentage of agreement (how many entities have this value)
		Confidence float32 `json:"confidence"` // Confidence in the value
	} `json:"sample_fields"`
}

// MatchGroupOptions represents options for match group retrieval
type MatchGroupOptions struct {
	ThresholdOverride float32            // Optional threshold override for group membership
	MaxGroupSize      int                // Maximum group size to return (0 for unlimited)
	IncludeScores     bool               // Whether to include detailed scores
	Strategy          string             // Strategy for group retrieval: "transitive", "direct", "hybrid"
	HopsLimit         int                // Maximum number of transitive hops (for transitive strategy)
	FieldWeights      map[string]float32 // Field weights for scoring
}

// GetMatchGroup retrieves all entities that match/belong to the same group as the specified entity
func (s *Service) GetMatchGroup(ctx context.Context, entityID string, opts MatchGroupOptions) (*MatchGroup, error) {
	// Apply default options
	if opts.ThresholdOverride <= 0 {
		opts.ThresholdOverride = s.cfg.Matching.SimilarityThreshold
	}
	if opts.MaxGroupSize <= 0 {
		opts.MaxGroupSize = 100 // Reasonable default limit
	}
	if opts.Strategy == "" {
		opts.Strategy = "hybrid" // Default to hybrid strategy
	}
	if opts.HopsLimit <= 0 {
		opts.HopsLimit = 3 // Default to 3 hops
	}

	// Get the entity to match against
	entity, err := s.weaviateClient.GetEntity(ctx, entityID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve entity %s: %w", entityID, err)
	}

	//
	// Create the match group with the primary entity
	group := &MatchGroup{
		ID:        entityID,
		PrimaryID: entityID,
		Entities:  make([]MatchResult, 0),
		SampleFields: make(map[string]struct {
			Value      string  `json:"value"`
			Agreement  float32 `json:"agreement"`
			Confidence float32 `json:"confidence"`
		}),
	}

	// Add the primary entity to the group
	primaryResult := convertToMatchResult(entity, 1.0) // Primary entity has perfect score
	group.Entities = append(group.Entities, primaryResult)

	// Calculate match group based on the strategy
	switch opts.Strategy {
	case "direct":
		err = s.getDirectMatchGroup(ctx, group, entity, opts)
	case "transitive":
		err = s.getTransitiveMatchGroup(ctx, group, entity, opts)
	case "hybrid":
		err = s.getHybridMatchGroup(ctx, group, entity, opts)
	default:
		return nil, fmt.Errorf("unknown match group strategy: %s", opts.Strategy)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve match group: %w", err)
	}

	// Calculate aggregate statistics
	s.calculateGroupStatistics(group)

	return group, nil
}

// getDirectMatchGroup finds entities that directly match the primary entity
func (s *Service) getDirectMatchGroup(ctx context.Context, group *MatchGroup, entity *weaviate.EntityRecord, opts MatchGroupOptions) error {
	// Find direct matches for the entity
	matchOpts := Options{
		Limit:              opts.MaxGroupSize,
		Threshold:          opts.ThresholdOverride,
		IncludeDetails:     opts.IncludeScores,
		UseClustering:      s.cfg.Clustering.Enabled,
		IncludeFieldScores: opts.IncludeScores,
		FieldWeights:       opts.FieldWeights,
	}

	// Create EntityData from the EntityRecord for FindMatchesForEntity
	entityData := EntityData{
		ID: entity.ID,
		Fields: map[string]string{
			"name":    entity.Name,
			"address": entity.Address,
			"city":    entity.City,
			"state":   entity.State,
			"zip":     entity.Zip,
			"phone":   entity.Phone,
			"email":   entity.Email,
		},
		Metadata: entity.Metadata,
	}

	// Add normalized fields if available
	if entity.NameNormalized != "" {
		entityData.Fields["name_normalized"] = entity.NameNormalized
	}
	if entity.AddressNormalized != "" {
		entityData.Fields["address_normalized"] = entity.AddressNormalized
	}
	if entity.CityNormalized != "" {
		entityData.Fields["city_normalized"] = entity.CityNormalized
	}
	if entity.StateNormalized != "" {
		entityData.Fields["state_normalized"] = entity.StateNormalized
	}
	if entity.ZipNormalized != "" {
		entityData.Fields["zip_normalized"] = entity.ZipNormalized
	}
	if entity.PhoneNormalized != "" {
		entityData.Fields["phone_normalized"] = entity.PhoneNormalized
	}
	if entity.EmailNormalized != "" {
		entityData.Fields["email_normalized"] = entity.EmailNormalized
	}

	matches, err := s.FindMatchesForEntity(ctx, entityData, matchOpts)
	if err != nil {
		return fmt.Errorf("failed to find matches for entity: %w", err)
	}

	// Add matched entities to the group (excluding the primary entity)
	for _, match := range matches {
		if match.ID != entity.ID {
			group.Entities = append(group.Entities, match)
		}
	}

	return nil
}

// getTransitiveMatchGroup finds entities through transitive relationships
func (s *Service) getTransitiveMatchGroup(ctx context.Context, group *MatchGroup, entity *weaviate.EntityRecord, opts MatchGroupOptions) error {
	visited := make(map[string]bool)
	visited[entity.ID] = true

	// Breadth-first search to find transitive matches
	queue := []*weaviate.EntityRecord{entity}
	hopCount := make(map[string]int) // Track hop distance from primary entity
	hopCount[entity.ID] = 0

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Skip if we've reached the hop limit for this entity
		currentHops := hopCount[current.ID]
		if currentHops >= opts.HopsLimit {
			continue
		}

		// Find direct matches for the current entity
		entityData := EntityData{
			ID: current.ID,
			Fields: map[string]string{
				"name":    current.Name,
				"address": current.Address,
				"city":    current.City,
				"state":   current.State,
				"zip":     current.Zip,
				"phone":   current.Phone,
				"email":   current.Email,
			},
			Metadata: current.Metadata,
		}

		// Add normalized fields if available
		if current.NameNormalized != "" {
			entityData.Fields["name_normalized"] = current.NameNormalized
		}
		if current.AddressNormalized != "" {
			entityData.Fields["address_normalized"] = current.AddressNormalized
		}
		if current.CityNormalized != "" {
			entityData.Fields["city_normalized"] = current.CityNormalized
		}
		if current.StateNormalized != "" {
			entityData.Fields["state_normalized"] = current.StateNormalized
		}
		if current.ZipNormalized != "" {
			entityData.Fields["zip_normalized"] = current.ZipNormalized
		}
		if current.PhoneNormalized != "" {
			entityData.Fields["phone_normalized"] = current.PhoneNormalized
		}
		if current.EmailNormalized != "" {
			entityData.Fields["email_normalized"] = current.EmailNormalized
		}

		matchOpts := Options{
			Limit:              opts.MaxGroupSize,
			Threshold:          opts.ThresholdOverride,
			IncludeDetails:     opts.IncludeScores,
			UseClustering:      s.cfg.Clustering.Enabled,
			IncludeFieldScores: opts.IncludeScores,
			FieldWeights:       opts.FieldWeights,
		}

		matches, err := s.FindMatchesForEntity(ctx, entityData, matchOpts)
		if err != nil {
			return fmt.Errorf("failed to find matches for entity %s: %w", current.ID, err)
		}

		// Process matches
		for _, match := range matches {
			if !visited[match.ID] {
				visited[match.ID] = true

				// Add the match to the group
				match.Metadata["hop_distance"] = currentHops + 1
				group.Entities = append(group.Entities, match)

				// Check if we've reached the max group size
				if opts.MaxGroupSize > 0 && len(group.Entities) >= opts.MaxGroupSize {
					return nil
				}

				// Get the full entity to add to the BFS queue
				matchEntity, err := s.weaviateClient.GetEntity(ctx, match.ID)
				if err != nil {
					// Log the error but continue processing
					fmt.Printf("Warning: couldn't retrieve entity %s: %v\n", match.ID, err)
					continue
				}

				// Add to queue for BFS traversal
				queue = append(queue, matchEntity)
				hopCount[match.ID] = currentHops + 1
			}
		}
	}

	return nil
}

// getHybridMatchGroup combines direct and limited transitive matching
func (s *Service) getHybridMatchGroup(ctx context.Context, group *MatchGroup, entity *weaviate.EntityRecord, opts MatchGroupOptions) error {
	// First get direct matches with a higher threshold for high confidence matches
	directOpts := opts
	directOpts.HopsLimit = 1

	// Use a higher threshold for direct matches
	if directOpts.ThresholdOverride < 0.9 {
		directOpts.ThresholdOverride = 0.9
	}

	// Get high-confidence direct matches
	err := s.getDirectMatchGroup(ctx, group, entity, directOpts)
	if err != nil {
		return err
	}

	// Then use transitive closure but with limited hops
	if opts.HopsLimit > 1 {
		// Create a set of visited entities from the direct matches
		visited := make(map[string]bool)
		for _, entity := range group.Entities {
			visited[entity.ID] = true
		}

		// For each direct match, find its matches
		transitiveOpts := opts
		transitiveOpts.HopsLimit = opts.HopsLimit - 1 // Reduce hop limit

		// Use original threshold for transitive matches
		for _, directMatch := range group.Entities {
			if directMatch.ID == entity.ID {
				continue // Skip the primary entity
			}

			// Get the entity to use for further matching
			matchEntity, err := s.weaviateClient.GetEntity(ctx, directMatch.ID)
			if err != nil {
				// Log the error but continue processing
				fmt.Printf("Warning: couldn't retrieve entity %s: %v\n", directMatch.ID, err)
				continue
			}

			// Create a temporary group for this branch
			tempGroup := &MatchGroup{
				ID:        directMatch.ID,
				PrimaryID: directMatch.ID,
				Entities:  make([]MatchResult, 0),
			}

			// Get matches for this entity
			err = s.getTransitiveMatchGroup(ctx, tempGroup, matchEntity, transitiveOpts)
			if err != nil {
				return err
			}

			// Add non-visited entities to the main group
			for _, transitiveMatch := range tempGroup.Entities {
				if !visited[transitiveMatch.ID] {
					visited[transitiveMatch.ID] = true
					group.Entities = append(group.Entities, transitiveMatch)

					// Check if we've reached the max group size
					if opts.MaxGroupSize > 0 && len(group.Entities) >= opts.MaxGroupSize {
						return nil
					}
				}
			}
		}
	}

	return nil
}

// calculateGroupStatistics computes aggregate statistics for a match group
func (s *Service) calculateGroupStatistics(group *MatchGroup) {
	if len(group.Entities) == 0 {
		return
	}

	// Calculate average score
	var totalScore float32
	for _, entity := range group.Entities {
		totalScore += entity.Score
	}
	group.Score = totalScore / float32(len(group.Entities))
	group.Size = len(group.Entities)

	// Sort entities by score (highest first)
	sort.Slice(group.Entities, func(i, j int) bool {
		return group.Entities[i].Score > group.Entities[j].Score
	})

	// Calculate field value agreement
	fieldCounts := make(map[string]map[string]int) // field -> value -> count

	// Count occurrences of each value for each field
	for _, entity := range group.Entities {
		for field, value := range entity.Fields {
			// Skip normalized fields
			if field == "" || value == "" {
				continue
			}

			// Initialize map if needed
			if _, ok := fieldCounts[field]; !ok {
				fieldCounts[field] = make(map[string]int)
			}

			// Increment count for this value
			fieldCounts[field][value]++
		}
	}

	// Find the most common value for each field and calculate agreement percentage
	for field, valueCounts := range fieldCounts {
		maxCount := 0
		mostCommonValue := ""

		for value, count := range valueCounts {
			if count > maxCount {
				maxCount = count
				mostCommonValue = value
			}
		}

		if mostCommonValue != "" {
			// Calculate percentage of agreement and confidence
			agreement := float32(maxCount) / float32(len(group.Entities))

			// Add to sample fields
			group.SampleFields[field] = struct {
				Value      string  `json:"value"`
				Agreement  float32 `json:"agreement"`
				Confidence float32 `json:"confidence"`
			}{
				Value:      mostCommonValue,
				Agreement:  agreement,
				Confidence: agreement * group.Score, // Weight by group score
			}
		}
	}
}
