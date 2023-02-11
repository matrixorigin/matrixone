/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

// rootCmd represents the base command when called without any subcommands
/*var rootCmd = &cobra.Command{
	Use:   "mo-tool",
	Short: `mo-tool is a CLI library for MO CLuster`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}*/

var rootCmd = &cobra.Command{
	Use:   "mo-tool",
	Short: `mo-tool is a CLI library for MO CLuster`,
	Run: func(cmd *cobra.Command, args []string) {
		if version {
			if ShowVersion != nil {
				ShowVersion()
			} else {
				fmt.Fprintln(os.Stderr, "No version info.")
			}
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var ShowVersion func() = nil

var version bool

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mo-tool.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&version, "version", "v", false, "Print version information")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
