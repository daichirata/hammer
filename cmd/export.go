package cmd

import (
	"fmt"

	"github.com/daichirata/hammer/internal"
	"github.com/spf13/cobra"
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

			if internal.Scheme(databaseURI) != "spanner" {
				return fmt.Errorf("")
			}
			database, err := internal.NewSource(databaseURI)
			if err != nil {
				return err
			}

			ddls, err := database.Read()
			if err != nil {
				return err
			}
			fmt.Println(ddls)

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(exportCmd)
}
