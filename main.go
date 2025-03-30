package main

import (
	"log"

	"github.com/TFMV/resolve/cmd/resolve"
)

func main() {
	// Execute initializes all commands and starts the CLI
	resolve.Execute()
	log.Println("Resolve terminated")
}
