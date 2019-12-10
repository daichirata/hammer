package cmd

import (
	"fmt"

	"github.com/daichirata/hammer/internal"
	"github.com/spf13/cobra"
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

			source1, err := internal.NewSource(sourceURI1)
			if err != nil {
				return err
			}
			source2, err := internal.NewSource(sourceURI2)
			if err != nil {
				return err
			}

			ddls, err := internal.GenerateDDLs(source1, source2)
			if err != nil {
				return err
			}
			for _, ddl := range ddls {
				fmt.Println(ddl.SQL())
			}

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(diffCmd)
}
