package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	createCmd = &cobra.Command{
		Use:   "create DATABASE SOURCE",
		Short: "Create database and apply schema",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseURI := args[0]

			if hammer.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("DATABASE must be a spanner URI")
			}
			database, err := hammer.NewSpannerSource(databaseURI)
			if err != nil {
				return err
			}
			ddl, err := database.DDL()
			if err != nil {
				return err
			}
			return database.Create(ddl)
		},
	}
)

func init() {
	rootCmd.AddCommand(createCmd)
}
