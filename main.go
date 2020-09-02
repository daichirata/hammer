package main

import (
	"os"

	"github.com/daichirata/hammer/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
