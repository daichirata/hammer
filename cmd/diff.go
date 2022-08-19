package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	diffExample = `
* Compare local files
  hammer diff /path/to/file /another/path/to/file

* Compare local file against spanner schema
  hammer diff /path/to/file spanner://projects/projectId/instances/instanceId/databases/databaseName

* Compare spanner schema against spanner schema
  hammer diff spanner://projects/projectId/instances/instanceId/databases/databaseName1 spanner://projects/projectId/instances/instanceId/databases/databaseName2`

	diffCmd = &cobra.Command{
		Use:     "diff SOURCE1 SOURCE2",
		Short:   "Diff schema",
		Example: diffExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			sourceURI1 := args[0]
			sourceURI2 := args[1]

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

			source1, err := hammer.NewSource(ctx, sourceURI1)
			if err != nil {
				return err
			}
			source2, err := hammer.NewSource(ctx, sourceURI2)
			if err != nil {
				return err
			}

			ddl1, err := source1.DDL(ctx, ddlOption)
			if err != nil {
				return err
			}
			ddl2, err := source2.DDL(ctx, ddlOption)
			if err != nil {
				return err
			}

			ddl, err := hammer.Diff(ddl1, ddl2)
			if err != nil {
				return err
			}
			for _, stmt := range ddl.List {
				fmt.Println(stmt.SQL() + ";")
			}
			return nil
		},
	}
)

func init() {
	diffCmd.Flags().Bool("ignore-alter-database", false, "ignore alter database statements")
	diffCmd.Flags().Bool("ignore-change-streams", false, "ignore change streams statements")

	rootCmd.AddCommand(diffCmd)
}
