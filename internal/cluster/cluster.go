package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/TFMV/resolve/internal/normalize"
	"github.com/TFMV/resolve/internal/weaviate"
)

// ClusterMethod defines the clustering algorithm to use
type ClusterMethod string

const (
	// CanopyMethod uses key-based canopy clustering
	CanopyMethod ClusterMethod = "canopy"
	// DefaultClusterID is used when no cluster can be determined
	DefaultClusterID = "default"
	// ClusterMetadataKey is the key used to store cluster ID in entity metadata
	ClusterMetadataKey = "cluster_id"
	// MaxClustersToSearch is the maximum number of clusters to search for a match
	MaxClustersToSearch = 3
)

// Config holds the clustering configuration
type Config struct {
	Enabled             bool     `mapstructure:"enabled"`
	Method              string   `mapstructure:"method"`
	Fields              []string `mapstructure:"fields"`
	SimilarityThreshold float64  `mapstructure:"similarity_threshold"`
}

// Service represents the clustering service
type Service struct {
	config     *Config
	normalizer *normalize.Normalizer
	keyCache   map[string]string
	cacheMutex sync.RWMutex
}

// NewService creates a new clustering service
func NewService(config *Config, normalizer *normalize.Normalizer) *Service {
	return &Service{
		config:     config,
		normalizer: normalizer,
		keyCache:   make(map[string]string),
	}
}

// GenerateClusterKey generates a cluster key for the given entity data
// It uses the configured fields to generate a blocking key
func (s *Service) GenerateClusterKey(ctx context.Context, fields map[string]string) string {
	if !s.config.Enabled || len(s.config.Fields) == 0 {
		return DefaultClusterID
	}

	// Sort fields for consistent keys
	fieldNames := make([]string, 0, len(s.config.Fields))
	for _, field := range s.config.Fields {
		if _, ok := fields[field]; ok {
			fieldNames = append(fieldNames, field)
		}
	}
	sort.Strings(fieldNames)

	// Generate cache key (for memoization)
	cacheKey := ""
	for _, field := range fieldNames {
		cacheKey += field + ":" + fields[field] + "|"
	}

	// Check cache first
	s.cacheMutex.RLock()
	if cacheValue, ok := s.keyCache[cacheKey]; ok {
		s.cacheMutex.RUnlock()
		return cacheValue
	}
	s.cacheMutex.RUnlock()

	// Normalize and concatenate field values
	var keyBuilder strings.Builder
	for _, field := range fieldNames {
		// Get normalized value
		normalizedField := fields[field+"_normalized"]
		if normalizedField == "" {
			normalizedField = fields[field]
		}

		// Extract blocking key components based on field type
		var keyComponent string
		switch field {
		case "name":
			// Extract first 3 characters for name if available
			if len(normalizedField) >= 3 {
				keyComponent = normalizedField[:3]
			} else {
				keyComponent = normalizedField
			}
		case "zip":
			// Extract first 5 characters for zip/postal code
			if len(normalizedField) >= 5 {
				keyComponent = normalizedField[:5]
			} else {
				keyComponent = normalizedField
			}
		case "phone":
			// Extract last 4 digits if at least 4 digits are available
			digits := extractDigits(normalizedField)
			if len(digits) >= 4 {
				keyComponent = digits[len(digits)-4:]
			} else {
				keyComponent = digits
			}
		case "email":
			// Use domain part of email
			if parts := strings.Split(normalizedField, "@"); len(parts) == 2 {
				keyComponent = parts[1]
			} else {
				keyComponent = normalizedField
			}
		default:
			// For other fields, use first 3 characters if available
			if len(normalizedField) >= 3 {
				keyComponent = normalizedField[:3]
			} else {
				keyComponent = normalizedField
			}
		}

		if keyComponent != "" {
			keyBuilder.WriteString(keyComponent)
			keyBuilder.WriteString("|")
		}
	}

	key := keyBuilder.String()
	if key == "" || key == "|" {
		return DefaultClusterID
	}

	// Create MD5 hash of the key
	hash := md5.Sum([]byte(key))
	clusterID := hex.EncodeToString(hash[:])[:16] // Use first 16 chars of the hash

	// Store in cache
	s.cacheMutex.Lock()
	s.keyCache[cacheKey] = clusterID
	s.cacheMutex.Unlock()

	return clusterID
}

// AssignCluster assigns a cluster ID to an entity
func (s *Service) AssignCluster(ctx context.Context, entity *weaviate.EntityRecord) (string, error) {
	// Skip if clustering is disabled
	if !s.config.Enabled {
		return DefaultClusterID, nil
	}

	// Extract fields from entity
	fields := map[string]string{
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
	}

	// Generate cluster key
	clusterID := s.GenerateClusterKey(ctx, fields)

	// Ensure metadata exists
	if entity.Metadata == nil {
		entity.Metadata = make(map[string]interface{})
	}

	// Set cluster ID in metadata
	entity.Metadata[ClusterMetadataKey] = clusterID

	return clusterID, nil
}

// RecomputeAllClusters recomputes clusters for all entities
func (s *Service) RecomputeAllClusters(ctx context.Context, client *weaviate.Client, batchSize int) error {
	if !s.config.Enabled {
		return nil
	}

	// Get all entities (paginated)
	offset := 0
	for {
		entities, err := client.ListEntities(ctx, offset, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list entities: %w", err)
		}

		// If no more entities, we're done
		if len(entities) == 0 {
			break
		}

		// Assign cluster to each entity
		updatedEntities := make([]*weaviate.EntityRecord, 0, len(entities))
		for _, entity := range entities {
			_, err := s.AssignCluster(ctx, entity)
			if err != nil {
				return fmt.Errorf("failed to assign cluster to entity %s: %w", entity.ID, err)
			}
			updatedEntities = append(updatedEntities, entity)
		}

		// Update entities in batch
		_, err = client.BatchUpdateEntities(ctx, updatedEntities)
		if err != nil {
			return fmt.Errorf("failed to update entities: %w", err)
		}

		// Move to next page
		offset += len(entities)

		// If we got fewer entities than the batch size, we're done
		if len(entities) < batchSize {
			break
		}
	}

	return nil
}

// GetClusterFilterForEntity returns a map of filters to search for similar clusters
func (s *Service) GetClusterFilterForEntity(ctx context.Context, entity *weaviate.EntityRecord) map[string]string {
	// Skip if clustering is disabled
	if !s.config.Enabled || entity.Metadata == nil {
		return nil
	}

	// Get cluster ID from metadata
	clusterID, ok := entity.Metadata[ClusterMetadataKey].(string)
	if !ok || clusterID == "" || clusterID == DefaultClusterID {
		return nil
	}

	// Create filter for the specific cluster ID
	return map[string]string{
		"metadata." + ClusterMetadataKey: clusterID,
	}
}

// Helper function to extract digits from a string
func extractDigits(s string) string {
	var digitsOnly strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digitsOnly.WriteRune(r)
		}
	}
	return digitsOnly.String()
}
