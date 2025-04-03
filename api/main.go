package api

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/resolve/internal/config"
)

// RunServer starts the API server and handles graceful shutdown
func RunServer(cfg *config.Config, embeddingDim int) error {
	// Create server
	server, err := NewServer(cfg, embeddingDim)
	if err != nil {
		return err
	}

	// Channel to listen for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Channel to handle server errors
	errCh := make(chan error)

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil {
			errCh <- err
		}
	}()

	// Wait for interrupt signal or error
	select {
	case <-stop:
		log.Println("Shutting down server...")
	case err := <-errCh:
		log.Printf("Server error: %v\n", err)
	}

	// Create a deadline for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v\n", err)
		return err
	}

	log.Println("Server gracefully stopped")
	return nil
}
