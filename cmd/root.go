package cmd

import (
	"strings"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:          "hammer",
		Short:        "hammer is a command-line tool to schema management for Google Cloud Spanner.",
		Example:      strings.Join([]string{exportExample, applyExample, createExample, diffExample}, "\n"),
		SilenceUsage: true,
	}
)

func Execute() error {
	return rootCmd.Execute()
}
