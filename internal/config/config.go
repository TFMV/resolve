package config

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds all configuration for the Resolve application
type Config struct {
	// Server configuration
	Server struct {
		Port int `mapstructure:"port"`
	} `mapstructure:"server"`

	// API configuration
	API struct {
		Host             string `mapstructure:"host"`
		Port             int    `mapstructure:"port"`
		ReadTimeoutSecs  int    `mapstructure:"read_timeout_secs"`
		WriteTimeoutSecs int    `mapstructure:"write_timeout_secs"`
		IdleTimeoutSecs  int    `mapstructure:"idle_timeout_secs"`
	} `mapstructure:"api"`

	// Weaviate configuration
	Weaviate struct {
		Host      string `mapstructure:"host"`
		Scheme    string `mapstructure:"scheme"`
		APIKey    string `mapstructure:"api_key"`
		ClassName string `mapstructure:"class_name"`
	} `mapstructure:"weaviate"`

	// Embedding service configuration
	Embedding struct {
		URL          string `mapstructure:"url"`
		BatchSize    int    `mapstructure:"batch_size"`
		Timeout      int    `mapstructure:"timeout"`
		CacheSize    int    `mapstructure:"cache_size"`
		ModelName    string `mapstructure:"model_name"`
		EmbeddingDim int    `mapstructure:"embedding_dim"`
	} `mapstructure:"embedding"`

	// Matching configuration
	Matching struct {
		SimilarityThreshold float32            `mapstructure:"similarity_threshold"`
		FieldWeights        map[string]float32 `mapstructure:"field_weights"`
		DefaultLimit        int                `mapstructure:"default_limit"`
	} `mapstructure:"matching"`

	// Normalization configuration
	Normalization struct {
		NameOptions     map[string]bool `mapstructure:"name_options"`
		AddressOptions  map[string]bool `mapstructure:"address_options"`
		PhoneOptions    map[string]bool `mapstructure:"phone_options"`
		EmailOptions    map[string]bool `mapstructure:"email_options"`
		EnableStopwords bool            `mapstructure:"enable_stopwords"`
		EnableStemming  bool            `mapstructure:"enable_stemming"`
		EnableLowercase bool            `mapstructure:"enable_lowercase"`
	} `mapstructure:"normalization"`
}

// Load loads the configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// If config file is provided, read it
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Look for config in the current directory
		v.AddConfigPath(".")
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	}

	// Read environment variables
	v.AutomaticEnv()
	v.SetEnvPrefix("RESOLVE")

	// Try to read config file (don't return error if not found)
	_ = v.ReadInConfig()

	// Unmarshal the config
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// setDefaults sets default values for the configuration
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.port", 8080)

	// API defaults
	v.SetDefault("api.host", "0.0.0.0")
	v.SetDefault("api.port", 8080)
	v.SetDefault("api.read_timeout_secs", 30)
	v.SetDefault("api.write_timeout_secs", 30)
	v.SetDefault("api.idle_timeout_secs", 60)

	// Weaviate defaults
	v.SetDefault("weaviate.host", "localhost:8080")
	v.SetDefault("weaviate.scheme", "http")
	v.SetDefault("weaviate.class_name", "Entity")

	// Embedding service defaults
	v.SetDefault("embedding.url", "http://localhost:8000")
	v.SetDefault("embedding.batch_size", 32)
	v.SetDefault("embedding.timeout", 30)
	v.SetDefault("embedding.cache_size", 1000)
	v.SetDefault("embedding.model_name", "all-MiniLM-L6-v2")
	v.SetDefault("embedding.embedding_dim", 384)

	// Matching defaults
	v.SetDefault("matching.similarity_threshold", 0.85)
	v.SetDefault("matching.default_limit", 10)
	v.SetDefault("matching.field_weights", map[string]float32{
		"name":    0.4,
		"address": 0.2,
		"city":    0.1,
		"state":   0.05,
		"zip":     0.05,
		"phone":   0.1,
		"email":   0.1,
	})

	// Normalization defaults
	v.SetDefault("normalization.enable_stopwords", true)
	v.SetDefault("normalization.enable_stemming", true)
	v.SetDefault("normalization.enable_lowercase", true)
	v.SetDefault("normalization.name_options", map[string]bool{
		"remove_legal_suffixes": true,
		"normalize_initials":    true,
	})
	v.SetDefault("normalization.address_options", map[string]bool{
		"standardize_abbreviations": true,
		"remove_apartment_numbers":  true,
	})
	v.SetDefault("normalization.phone_options", map[string]bool{
		"e164_format": true,
	})
	v.SetDefault("normalization.email_options", map[string]bool{
		"lowercase_domain": true,
	})
}

// SaveDefault saves the default configuration to a file
func SaveDefault(configPath string) error {
	v := viper.New()
	setDefaults(v)

	// Ensure directory exists
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return v.WriteConfigAs(configPath)
}
