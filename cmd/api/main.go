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

	// Run API server
	if err := api.Run(cfg); err != nil {
		log.Fatalf("API server error: %v", err)
	}
}
