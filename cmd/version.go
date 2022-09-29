package cmd

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var Version string

var (
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Display version",
		Run: func(cmd *cobra.Command, args []string) {
			if Version != "" {
				fmt.Println(Version)
				return
			}
			if buildInfo, ok := debug.ReadBuildInfo(); ok {
				fmt.Println(buildInfo.Main.Version)
				return
			}
			fmt.Println("(unknown)")
		},
	}
)

func init() {
	rootCmd.AddCommand(versionCmd)
}
