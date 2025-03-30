package resolve

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/qdrant"
	"github.com/spf13/cobra"
)

var (
	idColumnName     string
	textColumnName   string
	format           string
	batchSize        int
	collectionName   string
	createCollection bool
	skipHeaderRow    bool
)

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest [file]",
	Short: "Ingest entities from a file",
	Long: `Ingest entities from a CSV or JSON file into the Qdrant database.
The file should contain entity IDs and text values to be matched against.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize clients
		qdrantClient, err := qdrant.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize Qdrant client: %w", err)
		}
		defer qdrantClient.Close()

		// Check if collection exists, create if requested
		if createCollection {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := qdrantClient.CreateCollection(ctx); err != nil {
				return fmt.Errorf("failed to create collection: %w", err)
			}
		}

		// Initialize embedding service
		embeddingService := embed.NewEmbeddingService(cfg)

		// Initialize matching service
		matchService := match.NewService(cfg, qdrantClient, embeddingService)

		// Open input file
		file, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		// Process based on format
		switch strings.ToLower(format) {
		case "csv":
			return processCSV(matchService, file)
		case "json":
			return processJSON(matchService, file)
		default:
			return fmt.Errorf("unsupported format: %s", format)
		}
	},
}

func init() {
	rootCmd.AddCommand(ingestCmd)

	ingestCmd.Flags().StringVar(&idColumnName, "id-column", "id", "Column name for entity IDs (CSV only)")
	ingestCmd.Flags().StringVar(&textColumnName, "text-column", "text", "Column name for entity text (CSV only)")
	ingestCmd.Flags().StringVar(&format, "format", "csv", "File format (csv or json)")
	ingestCmd.Flags().IntVar(&batchSize, "batch-size", 100, "Number of entities to process in each batch")
	ingestCmd.Flags().BoolVar(&createCollection, "create-collection", false, "Create collection if it doesn't exist")
	ingestCmd.Flags().BoolVar(&skipHeaderRow, "skip-header", true, "Skip header row (CSV only)")
}

func processCSV(matchService *match.Service, file io.Reader) error {
	reader := csv.NewReader(file)

	// Read header row
	var header []string
	if skipHeaderRow {
		var err error
		header, err = reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV header: %w", err)
		}
	}

	// Find column indices
	idIdx := -1
	textIdx := -1

	if skipHeaderRow {
		for i, col := range header {
			if col == idColumnName {
				idIdx = i
			}
			if col == textColumnName {
				textIdx = i
			}
		}

		if textIdx == -1 {
			return fmt.Errorf("text column '%s' not found in CSV header", textColumnName)
		}
	} else {
		// If no header, assume id is column 0 and text is column 1
		idIdx = 0
		textIdx = 1
	}

	// Process rows in batches
	batch := make([]match.EntityData, 0, batchSize)
	rowCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV: %w", err)
		}

		// Extract data
		var id, text string

		if idIdx >= 0 && idIdx < len(record) {
			id = record[idIdx]
		}

		if textIdx >= 0 && textIdx < len(record) {
			text = record[textIdx]
		} else {
			return fmt.Errorf("text column index out of range: %d", textIdx)
		}

		// Skip empty text
		if strings.TrimSpace(text) == "" {
			continue
		}

		// Add to batch
		batch = append(batch, match.EntityData{
			ID:           id,
			OriginalText: text,
		})

		// Process batch if full
		if len(batch) >= batchSize {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			err := matchService.AddEntities(ctx, batch)
			cancel()

			if err != nil {
				return fmt.Errorf("failed to add entities: %w", err)
			}

			fmt.Printf("Processed %d entities\n", rowCount+len(batch))
			rowCount += len(batch)
			batch = batch[:0]
		}
	}

	// Process remaining batch
	if len(batch) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := matchService.AddEntities(ctx, batch)
		cancel()

		if err != nil {
			return fmt.Errorf("failed to add entities: %w", err)
		}

		fmt.Printf("Processed %d entities\n", rowCount+len(batch))
	}

	return nil
}

func processJSON(matchService *match.Service, file io.Reader) error {
	// Read JSON array
	var entities []struct {
		ID   string `json:"id"`
		Text string `json:"text"`
	}

	if err := json.NewDecoder(file).Decode(&entities); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Process in batches
	total := len(entities)
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		// Convert to EntityData
		batch := make([]match.EntityData, 0, end-i)
		for _, entity := range entities[i:end] {
			if strings.TrimSpace(entity.Text) == "" {
				continue
			}

			batch = append(batch, match.EntityData{
				ID:           entity.ID,
				OriginalText: entity.Text,
			})
		}

		// Skip empty batches
		if len(batch) == 0 {
			continue
		}

		// Process batch
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := matchService.AddEntities(ctx, batch)
		cancel()

		if err != nil {
			return fmt.Errorf("failed to add entities: %w", err)
		}

		fmt.Printf("Processed %d entities\n", i+len(batch))
	}

	return nil
}
