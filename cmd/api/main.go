package main

import (
	"flag"
	"log"

	"github.com/TFMV/resolve/api"
	"github.com/TFMV/resolve/internal/config"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Get the embedding dimension
	embeddingDim := cfg.Embedding.EmbeddingDim

	// Run API server
	if err := api.RunServer(cfg, embeddingDim); err != nil {
		log.Fatalf("API server error: %v", err)
	}
}
