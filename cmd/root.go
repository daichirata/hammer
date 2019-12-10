package cmd

import (
	"fmt"

	"github.com/daichirata/hammer/internal"
	"github.com/spf13/cobra"
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

			if internal.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("DATABASE must be a spanner URI")
			}
			database, err := internal.NewSpannerSource(databaseURI)
			if err != nil {
				return err
			}
			source, err := internal.NewSource(sourceURI)
			if err != nil {
				return err
			}

			ddls, err := internal.GenerateDDLs(database, source)
			if err != nil {
				return err
			}
			if len(ddls) == 0 {
				return nil
			}

			if err := database.Apply(ddls); err != nil {
				return err
			}
			return nil
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}
