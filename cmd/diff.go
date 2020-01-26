package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/daichirata/hammer/internal/hammer"
)

var (
	diffCmd = &cobra.Command{
		Use:   "diff SOURCE1 SOURCE2",
		Short: "Diff schema",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("must specify 2 arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceURI1 := args[0]
			sourceURI2 := args[1]

			source1, err := hammer.NewSource(sourceURI1)
			if err != nil {
				return err
			}
			source2, err := hammer.NewSource(sourceURI2)
			if err != nil {
				return err
			}

			ddl, err := hammer.Diff(source1, source2)
			if err != nil {
				return err
			}
			for _, stmt := range ddl.List {
				fmt.Println(stmt.SQL())
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(diffCmd)
}
