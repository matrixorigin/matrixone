// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ckp

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool/interactive"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ckp [directory]",
		Short: "Checkpoint viewer tool",
		Long:  "Tools for analyzing and browsing MatrixOne checkpoint files",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runViewer(dir)
		},
	}

	cmd.AddCommand(infoCommand())
	cmd.AddCommand(viewCommand())

	return cmd
}

func runViewer(dir string) error {
	ctx := context.Background()
	reader, err := checkpointtool.Open(ctx, dir)
	if err != nil {
		return fmt.Errorf("open checkpoint dir: %w", err)
	}
	defer reader.Close()

	return interactive.Run(reader)
}

func infoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info [directory]",
		Short: "Show checkpoint summary",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}

			ctx := context.Background()
			reader, err := checkpointtool.Open(ctx, dir)
			if err != nil {
				return fmt.Errorf("open checkpoint dir: %w", err)
			}
			defer reader.Close()

			info := reader.Info()
			fmt.Printf("Checkpoint Directory: %s\n", info.Dir)
			fmt.Printf("Total Entries: %d\n", info.TotalEntries)
			fmt.Printf("  Global:      %d\n", info.GlobalCount)
			fmt.Printf("  Incremental: %d\n", info.IncrCount)
			fmt.Printf("  Compacted:   %d\n", info.CompactCount)
			if !info.EarliestTS.IsEmpty() {
				fmt.Printf("Earliest TS:   %s\n", info.EarliestTS.ToString())
			}
			if !info.LatestTS.IsEmpty() {
				fmt.Printf("Latest TS:     %s\n", info.LatestTS.ToString())
			}
			return nil
		},
	}
}

func viewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "view [directory]",
		Short: "Interactive checkpoint viewer",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runViewer(dir)
		},
	}
}
