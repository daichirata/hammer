package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	rootCmd = &cobra.Command{
		Use:   "hammer DATABASE SOURCE",
		Short: "hammer is a command-line tool to schema management for Google Cloud Spanner.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseURI := args[0]
			sourceURI := args[1]

			if hammer.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("DATABASE must be a spanner URI")
			}
			database, err := hammer.NewSpannerSource(databaseURI)
			if err != nil {
				return err
			}
			source, err := hammer.NewSource(sourceURI)
			if err != nil {
				return err
			}

			ddl, err := hammer.Diff(database, source)
			if err != nil {
				return err
			}
			if len(ddl.List) == 0 {
				return nil
			}

			if err := database.Apply(ddl); err != nil {
				return err
			}
			return nil
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}
