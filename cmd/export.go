package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	exportCmd = &cobra.Command{
		Use:   "export DATABASE",
		Short: "Export schema",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("must specify 1 argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseURI := args[0]

			if hammer.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("DATABASE must be a spanner URI")
			}
			database, err := hammer.NewSource(databaseURI)
			if err != nil {
				return err
			}

			ddl, err := database.DDL()
			if err != nil {
				return err
			}
			for _, stmt := range ddl.List {
				fmt.Println(stmt.SQL() + ";\n")
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(exportCmd)
}
