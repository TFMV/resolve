package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/weaviate"
)

const (
	defaultConfigPath = "config.yaml"
	version           = "0.1.0"
)

var (
	configPath        string
	showVersion       bool
	ingestFile        string
	matchFile         string
	matchString       string
	threshold         float64
	limit             int
	withDetails       bool
	showHelp          bool
	recomputeClusters bool
	groupID           string
	groupStrategy     string
	groupHopsLimit    int
	fieldScores       bool
)

func main() {
	// Define command-line flags
	flag.StringVar(&configPath, "config", defaultConfigPath, "Path to configuration file")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.StringVar(&ingestFile, "ingest", "", "Path to JSON file with entities to ingest")
	flag.StringVar(&matchFile, "match-file", "", "Path to JSON file with entity to match")
	flag.StringVar(&matchString, "match", "", "Entity string to match")
	flag.Float64Var(&threshold, "threshold", 0, "Match threshold (0.0-1.0)")
	flag.IntVar(&limit, "limit", 0, "Maximum number of matches to return")
	flag.BoolVar(&withDetails, "details", false, "Include match details")
	flag.BoolVar(&showHelp, "help", false, "Show help information")
	flag.BoolVar(&recomputeClusters, "recompute-clusters", false, "Recompute clusters for all entities")
	flag.StringVar(&groupID, "group", "", "Find match group for the specified entity ID")
	flag.StringVar(&groupStrategy, "group-strategy", "direct", "Group strategy: direct, transitive, or hybrid")
	flag.IntVar(&groupHopsLimit, "group-hops", 2, "Maximum number of hops for transitive matching")
	flag.BoolVar(&fieldScores, "field-scores", false, "Enable field-level similarity scoring")
	flag.Parse()

	// Check for help flag
	if showHelp {
		printUsage()
		os.Exit(0)
	}

	// Check for version flag
	if showVersion {
		fmt.Printf("Resolve Entity Matching System v%s\n", version)
		os.Exit(0)
	}

	// Ensure at least one command is specified
	if ingestFile == "" && matchFile == "" && matchString == "" && !recomputeClusters && groupID == "" {
		log.Fatal("Error: No command specified. Use --help for usage information.")
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Config file not found at %s, using defaults", configPath)
			cfg = defaultConfig()
		} else {
			log.Fatalf("Error loading config: %v", err)
		}
	}

	// Set up context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize embedding service
	embeddingService := embed.NewHTTPClient(cfg)

	// Initialize Weaviate client
	weaviateClient, err := weaviate.NewClient(cfg, cfg.Embedding.EmbeddingDim)
	if err != nil {
		log.Fatalf("Error initializing Weaviate client: %v", err)
	}

	// Check connection to Weaviate
	healthy, err := weaviateClient.Health(ctx)
	if err != nil || !healthy {
		log.Fatalf("Error connecting to Weaviate: %v", err)
	}

	// Initialize matching service
	matchService := match.NewService(cfg, weaviateClient, embeddingService)

	// Process commands
	if ingestFile != "" {
		processIngest(ctx, matchService, ingestFile)
	}

	if matchFile != "" {
		processMatchFile(ctx, matchService, matchFile, threshold, limit, withDetails, fieldScores)
	}

	if matchString != "" {
		processMatchString(ctx, matchService, matchString, threshold, limit, withDetails, fieldScores)
	}

	if recomputeClusters {
		processRecomputeClusters(ctx, matchService)
	}

	if groupID != "" {
		processMatchGroup(ctx, matchService, groupID, threshold, groupStrategy, groupHopsLimit)
	}
}

// processIngest processes entity ingestion
func processIngest(ctx context.Context, matchService *match.Service, filePath string) {
	// Read and parse the ingest file
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading ingest file: %v", err)
	}

	var entities []match.EntityData
	if err := json.Unmarshal(data, &entities); err != nil {
		log.Fatalf("Error parsing ingest file: %v", err)
	}

	// Log start
	log.Printf("Ingesting %d entities", len(entities))
	startTime := time.Now()

	// Process entities
	err = matchService.AddEntities(ctx, entities)
	if err != nil {
		log.Fatalf("Error ingesting entities: %v", err)
	}

	// Log completion
	duration := time.Since(startTime)
	log.Printf("Successfully ingested %d entities in %.2f seconds", len(entities), duration.Seconds())
}

// processMatchFile matches entities from a file
func processMatchFile(ctx context.Context, matchService *match.Service, filePath string, threshold float64, limit int, withDetails bool, fieldScores bool) {
	// Read and parse the match file
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading match file: %v", err)
	}

	var entity match.EntityData
	if err := json.Unmarshal(data, &entity); err != nil {
		log.Fatalf("Error parsing match file: %v", err)
	}

	// Set up match options
	opts := match.Options{
		Threshold:          float32(threshold),
		Limit:              limit,
		IncludeDetails:     withDetails,
		IncludeFieldScores: fieldScores,
	}

	// Search for matches
	log.Printf("Searching for matches for entity %s", entity.ID)
	startTime := time.Now()

	matches, err := matchService.FindMatchesForEntity(ctx, entity, opts)
	if err != nil {
		log.Fatalf("Error searching for matches: %v", err)
	}

	// Log and output results
	duration := time.Since(startTime)
	log.Printf("Found %d matches in %.2f seconds", len(matches), duration.Seconds())

	if len(matches) > 0 {
		printMatches(matches)
	} else {
		fmt.Println("No matches found.")
	}
}

// processMatchString matches a string query
func processMatchString(ctx context.Context, matchService *match.Service, queryString string, threshold float64, limit int, withDetails bool, fieldScores bool) {
	// Set up match options
	opts := match.Options{
		Threshold:          float32(threshold),
		Limit:              limit,
		IncludeDetails:     withDetails,
		IncludeFieldScores: fieldScores,
	}

	// Search for matches
	log.Printf("Searching for matches for string query")
	startTime := time.Now()

	matches, err := matchService.FindMatches(ctx, queryString, opts)
	if err != nil {
		log.Fatalf("Error searching for matches: %v", err)
	}

	// Log and output results
	duration := time.Since(startTime)
	log.Printf("Found %d matches in %.2f seconds", len(matches), duration.Seconds())

	if len(matches) > 0 {
		printMatches(matches)
	} else {
		fmt.Println("No matches found.")
	}
}

// processMatchGroup finds all entities in the same match group
func processMatchGroup(ctx context.Context, matchService *match.Service, entityID string, threshold float64, strategy string, hopsLimit int) {
	// Set up group options
	opts := match.MatchGroupOptions{
		ThresholdOverride: float32(threshold),
		Strategy:          strategy,
		HopsLimit:         hopsLimit,
		IncludeScores:     true,
	}

	// Log start
	log.Printf("Finding match group for entity %s using %s strategy", entityID, strategy)
	startTime := time.Now()

	// Get the match group
	group, err := matchService.GetMatchGroup(ctx, entityID, opts)
	if err != nil {
		log.Fatalf("Error finding match group: %v", err)
	}

	// Log and output results
	duration := time.Since(startTime)
	log.Printf("Found match group with %d entities in %.2f seconds", group.Size, duration.Seconds())

	// Print group details
	output, err := json.MarshalIndent(group, "", "  ")
	if err != nil {
		log.Fatalf("Error formatting results: %v", err)
	}
	fmt.Println(string(output))
}

// processRecomputeClusters handles recomputing clusters for all entities
func processRecomputeClusters(ctx context.Context, matchService *match.Service) {
	// Log start
	log.Printf("Starting cluster recomputation for all entities")
	startTime := time.Now()

	// Recompute clusters
	err := matchService.RecomputeClusters(ctx)
	if err != nil {
		log.Fatalf("Error recomputing clusters: %v", err)
	}

	// Log completion
	duration := time.Since(startTime)
	log.Printf("Successfully recomputed clusters in %.2f seconds", duration.Seconds())
}

// printMatches outputs match results in JSON format
func printMatches(matches []match.MatchResult) {
	output, err := json.MarshalIndent(matches, "", "  ")
	if err != nil {
		log.Fatalf("Error formatting results: %v", err)
	}
	fmt.Println(string(output))
}

// defaultConfig returns a default configuration
func defaultConfig() *config.Config {
	cfg := &config.Config{}

	// Server defaults
	cfg.Server.Port = 8080

	// Weaviate defaults
	cfg.Weaviate.Host = "localhost:8080"
	cfg.Weaviate.Scheme = "http"
	cfg.Weaviate.ClassName = "Entity"

	// Embedding service defaults
	cfg.Embedding.URL = "http://localhost:8000"
	cfg.Embedding.BatchSize = 32
	cfg.Embedding.Timeout = 30
	cfg.Embedding.CacheSize = 1000
	cfg.Embedding.ModelName = "all-MiniLM-L6-v2"
	cfg.Embedding.EmbeddingDim = 384

	// Matching defaults
	cfg.Matching.SimilarityThreshold = 0.85
	cfg.Matching.DefaultLimit = 10
	cfg.Matching.FieldWeights = map[string]float32{
		"name":    0.4,
		"address": 0.2,
		"city":    0.1,
		"state":   0.05,
		"zip":     0.05,
		"phone":   0.1,
		"email":   0.1,
	}

	// Normalization defaults
	cfg.Normalization.EnableStopwords = true
	cfg.Normalization.EnableStemming = true
	cfg.Normalization.EnableLowercase = true
	cfg.Normalization.NameOptions = map[string]bool{
		"remove_legal_suffixes": true,
		"normalize_initials":    true,
	}
	cfg.Normalization.AddressOptions = map[string]bool{
		"standardize_abbreviations": true,
		"remove_apartment_numbers":  true,
	}
	cfg.Normalization.PhoneOptions = map[string]bool{
		"e164_format": true,
	}
	cfg.Normalization.EmailOptions = map[string]bool{
		"lowercase_domain": true,
	}

	return cfg
}

// printUsage prints usage information
func printUsage() {
	fmt.Println("Resolve Entity Matching System")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  resolve [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --config string            Path to configuration file (default \"config.yaml\")")
	fmt.Println("  --ingest string            Path to JSON file with entities to ingest")
	fmt.Println("  --match-file string        Path to JSON file with entity to match")
	fmt.Println("  --match string             Entity string to match")
	fmt.Println("  --threshold float          Match threshold (0.0-1.0)")
	fmt.Println("  --limit int                Maximum number of matches to return")
	fmt.Println("  --details                  Include match details")
	fmt.Println("  --field-scores             Include field-level similarity scores")
	fmt.Println("  --recompute-clusters       Recompute clusters for all entities")
	fmt.Println("  --group string             Find match group for the specified entity ID")
	fmt.Println("  --group-strategy string    Group strategy: direct, transitive, or hybrid (default \"direct\")")
	fmt.Println("  --group-hops int           Maximum number of hops for transitive matching (default 2)")
	fmt.Println("  --version                  Show version information")
	fmt.Println("  --help                     Show this help information")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  resolve --ingest entities.json")
	fmt.Println("  resolve --match-file query.json --threshold 0.8 --limit 5")
	fmt.Println("  resolve --match \"Acme Corporation\" --threshold 0.7")
	fmt.Println("  resolve --recompute-clusters")
	fmt.Println("  resolve --group entity-123 --group-strategy transitive --group-hops 3")
	fmt.Println("  resolve --match-file query.json --field-scores")
}
