package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	applyExample = `
* Apply local schema file
  hammer apply spanner://projects/projectId/instances/instanceId/databases/databaseName /path/to/file`

	applyCmd = &cobra.Command{
		Use:     "apply DATABASE SOURCE",
		Short:   "Apply schema",
		Example: applyExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 arguments")
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

			databaseDDL, err := database.DDL(ctx, ddlOption)
			if err != nil {
				return err
			}
			sourceDDL, err := source.DDL(ctx, ddlOption)
			if err != nil {
				return err
			}

			ddl, err := hammer.Diff(databaseDDL, sourceDDL)
			if err != nil {
				return err
			}
			if len(ddl.List) == 0 {
				return nil
			}

			if err := database.Apply(ctx, ddl); err != nil {
				return err
			}
			return nil
		},
	}
)

func init() {
	applyCmd.Flags().Bool("ignore-alter-database", false, "ignore alter database statements")
	applyCmd.Flags().Bool("ignore-change-streams", false, "ignore change streams statements")

	rootCmd.AddCommand(applyCmd)
}
