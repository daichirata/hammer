package cmd

import (
	"context"
	"fmt"

	"github.com/daichirata/hammer/internal/hammer"
	"github.com/spf13/cobra"
)

var (
	createExample = `
* Create database and apply local schema (faster than running database creation and schema apply separately)
  hammer create spanner://projects/projectId/instances/instanceId/databases/databaseName /path/to/file

* Copy database
  hammer create spanner://projects/projectId/instances/instanceId/databases/databaseName1 spanner://projects/projectId/instances/instanceId/databases/databaseName2`

	createCmd = &cobra.Command{
		Use:     "create DATABASE SOURCE",
		Short:   "Create database and apply schema",
		Example: createExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			databaseURI := args[0]
			sourceURI := args[1]

			ignoreAlterDatabase, err := cmd.Flags().GetBool("ignore-alter-database")
			if err != nil {
				return err
			}
			ignoreChangeStreams, err := cmd.Flags().GetBool("ignore-change-streams")
			if err != nil {
				return err
			}
			ddlOption := &hammer.DDLOption{
				IgnoreAlterDatabase: ignoreAlterDatabase,
				IgnoreChangeStreams: ignoreChangeStreams,
			}

			if hammer.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("DATABASE must be a spanner URI")
			}
			database, err := hammer.NewSpannerSource(ctx, databaseURI)
			if err != nil {
				return err
			}
			source, err := hammer.NewSource(ctx, sourceURI)
			if err != nil {
				return err
			}

			ddl, err := source.DDL(ctx, ddlOption)
			if err != nil {
				return err
			}
			return database.Create(ctx, ddl)
		},
	}
)

func init() {
	createCmd.Flags().Bool("ignore-alter-database", false, "ignore alter database statements")
	createCmd.Flags().Bool("ignore-change-streams", false, "ignore change streams statements")

	rootCmd.AddCommand(createCmd)
}
