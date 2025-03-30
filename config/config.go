package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	// Qdrant configuration
	QdrantHost   string `mapstructure:"QDRANT_HOST"`
	QdrantPort   int    `mapstructure:"QDRANT_PORT"`
	QdrantAPIKey string `mapstructure:"QDRANT_API_KEY"`
	QdrantUseTLS bool   `mapstructure:"QDRANT_USE_TLS"`

	// Embedding service configuration
	EmbeddingServiceURL  string `mapstructure:"EMBEDDING_SERVICE_URL"`
	EmbeddingServicePort int    `mapstructure:"EMBEDDING_SERVICE_PORT"`

	// Matching configuration
	SimilarityThreshold float32 `mapstructure:"SIMILARITY_THRESHOLD"`
	CollectionName      string  `mapstructure:"COLLECTION_NAME"`
	VectorSize          uint64  `mapstructure:"VECTOR_SIZE"`

	// API configuration
	ServerPort int `mapstructure:"SERVER_PORT"`
}

// LoadConfig loads application configuration from environment variables or config file
func LoadConfig() (*Config, error) {
	// Try to load .env file if it exists
	_, err := os.Stat(".env")
	if err == nil {
		err = godotenv.Load()
		if err != nil {
			log.Printf("Error loading .env file: %v", err)
		}
	}

	// Look for config file
	viper.SetConfigName("resolve")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.resolve")
	viper.AddConfigPath("/etc/resolve")

	// Read config file if it exists
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	// Set defaults
	viper.SetDefault("QDRANT_HOST", "localhost")
	viper.SetDefault("QDRANT_PORT", 6334)
	viper.SetDefault("QDRANT_USE_TLS", false)
	viper.SetDefault("EMBEDDING_SERVICE_URL", "http://localhost")
	viper.SetDefault("EMBEDDING_SERVICE_PORT", 8000)
	viper.SetDefault("SIMILARITY_THRESHOLD", 0.85)
	viper.SetDefault("COLLECTION_NAME", "entities")
	viper.SetDefault("VECTOR_SIZE", 768) // Common embedding size
	viper.SetDefault("SERVER_PORT", 8080)

	// Bind environment variables
	viper.AutomaticEnv()

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// CreateDefaultConfigFile creates a default configuration file
func CreateDefaultConfigFile(path string) error {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	content := `# Qdrant Configuration
QDRANT_HOST: localhost
QDRANT_PORT: 6334
QDRANT_API_KEY: ""
QDRANT_USE_TLS: false

# Embedding Service Configuration
EMBEDDING_SERVICE_URL: http://localhost
EMBEDDING_SERVICE_PORT: 8000

# Matching Configuration
SIMILARITY_THRESHOLD: 0.85
COLLECTION_NAME: "entities"
VECTOR_SIZE: 768

# Server Configuration
SERVER_PORT: 8080
`

	return os.WriteFile(path, []byte(content), 0644)
}
