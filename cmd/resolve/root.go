package resolve

import (
	"fmt"
	"os"

	"github.com/TFMV/resolve/config"
	"github.com/spf13/cobra"
)

var cfgFile string
var cfg *config.Config

// rootCmd represents the base command
var rootCmd = &cobra.Command{
	Use:   "resolve",
	Short: "Resolve - approximate entity matching system",
	Long: `Resolve is a production-grade approximate entity matching system 
that uses vector embeddings and Qdrant to provide high-quality fuzzy matching
and identity resolution at scale.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./resolve.yaml)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}
}
