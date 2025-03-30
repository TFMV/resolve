package resolve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/qdrant"
	"github.com/spf13/cobra"
)

var (
	limit          int
	threshold      float32
	includeDetails bool
	outputFormat   string
)

// matchCmd represents the match command
var matchCmd = &cobra.Command{
	Use:   "match [text]",
	Short: "Find matches for a text string",
	Long: `Find entities in the database that match the given text using 
vector similarity search.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize clients
		qdrantClient, err := qdrant.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("failed to initialize Qdrant client: %w", err)
		}
		defer qdrantClient.Close()

		// Initialize embedding service
		embeddingService := embed.NewEmbeddingService(cfg)

		// Initialize matching service
		matchService := match.NewService(cfg, qdrantClient, embeddingService)

		// Set options
		opts := match.Options{
			Limit:          limit,
			Threshold:      threshold,
			IncludeDetails: includeDetails,
		}

		// Perform search
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		matches, err := matchService.FindMatches(ctx, args[0], opts)
		if err != nil {
			return fmt.Errorf("failed to find matches: %w", err)
		}

		// Output results based on format
		if outputFormat == "json" {
			return outputJSON(matches)
		} else {
			return outputText(matches, args[0])
		}
	},
}

func init() {
	rootCmd.AddCommand(matchCmd)

	matchCmd.Flags().IntVar(&limit, "limit", 10, "Maximum number of matches to return")
	matchCmd.Flags().Float32Var(&threshold, "threshold", 0.0, "Minimum similarity score (0-1)")
	matchCmd.Flags().BoolVar(&includeDetails, "details", false, "Include normalization details")
	matchCmd.Flags().StringVar(&outputFormat, "format", "text", "Output format (text or json)")
}

func outputJSON(matches []match.Match) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(matches)
}

func outputText(matches []match.Match, query string) error {
	fmt.Printf("Query: %s\n\n", query)

	if len(matches) == 0 {
		fmt.Println("No matches found.")
		return nil
	}

	fmt.Printf("Found %d matches:\n\n", len(matches))

	for i, match := range matches {
		fmt.Printf("%d. %s (ID: %s)\n", i+1, match.OriginalText, match.ID)
		fmt.Printf("   Score: %.4f\n", match.Score)
		fmt.Printf("   Normalized: %s\n", match.Normalized)

		if match.NormalizeInfo != nil {
			fmt.Printf("   Normalization Details:\n")
			fmt.Printf("     - Lowercased: %t\n", match.NormalizeInfo.LowerCased)
			fmt.Printf("     - Punctuation Fixed: %t\n", match.NormalizeInfo.PunctuationFixed)
			fmt.Printf("     - Abbreviations Expanded: %t\n", match.NormalizeInfo.AbbreviationsExpanded)
		}

		fmt.Println()
	}

	return nil
}
