package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	exportExample = `
* Export spanner schema
  hammer export spanner://projects/projectId/instances/instanceId/databases/databaseName > schema.sql`

	exportCmd = &cobra.Command{
		Use:     "export SOURCE",
		Short:   "Export schema",
		Example: exportExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("must specify 1 argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			sourceURI := args[0]

			ignoreAlterDatabase, err := cmd.Flags().GetBool("ignore-alter-database")
			if err != nil {
				return err
			}
			ignoreChangeStreams, err := cmd.Flags().GetBool("ignore-change-streams")
			if err != nil {
				return err
			}
			ignoreModels, err := cmd.Flags().GetBool("ignore-models")
			if err != nil {
				return err
			}
			ignoreSequences, err := cmd.Flags().GetBool("ignore-sequences")
			if err != nil {
				return err
			}
			ddlOption := &hammer.DDLOption{
				IgnoreAlterDatabase: ignoreAlterDatabase,
				IgnoreChangeStreams: ignoreChangeStreams,
				IgnoreModels:        ignoreModels,
				IgnoreSequences:     ignoreSequences,
			}

			source, err := hammer.NewSource(ctx, sourceURI)
			if err != nil {
				return err
			}

			ddl, err := source.DDL(ctx, ddlOption)
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
	exportCmd.Flags().Bool("ignore-alter-database", false, "ignore alter database statements")
	exportCmd.Flags().Bool("ignore-change-streams", false, "ignore change streams statements")
	exportCmd.Flags().Bool("ignore-models", false, "ignore model statements")
	exportCmd.Flags().Bool("ignore-sequences", false, "ignore sequence statements")

	rootCmd.AddCommand(exportCmd)
}
