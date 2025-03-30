package resolve

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TFMV/resolve/api"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/qdrant"
	"github.com/spf13/cobra"
)

var (
	serverPort int
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the HTTP API server",
	Long: `Start the HTTP API server providing entity matching functionality
over HTTP endpoints for matching and ingestion.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize clients
		qdrantClient, err := qdrant.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize Qdrant client: %w", err)
		}
		defer qdrantClient.Close()

		// Check if Qdrant is healthy
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		version, err := qdrantClient.Health(ctx)
		if err != nil {
			log.Printf("Warning: Qdrant health check failed: %v", err)
		} else {
			log.Printf("Connected to Qdrant version: %s", version)
		}

		// Check if collection exists, create if not
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := qdrantClient.CreateCollection(ctx); err != nil {
			log.Printf("Warning: Failed to create collection: %v", err)
		}

		// Initialize embedding service
		embeddingService := embed.NewEmbeddingService(cfg)

		// Use configured port if not overridden
		if serverPort == 0 {
			serverPort = cfg.ServerPort
		}

		// Start API server
		server := api.NewServer(cfg, qdrantClient, embeddingService)
		return server.Start(serverPort)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().IntVar(&serverPort, "port", 0, "Server port (overrides config)")
}
