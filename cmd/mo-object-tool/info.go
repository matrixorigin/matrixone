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

package object

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/spf13/cobra"
)

func infoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <object-file>",
		Short: "Show object file information",
		Long:  "Display metadata and statistics about an object file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			return showInfo(path)
		},
	}

	return cmd
}

func showInfo(path string) error {
	ctx := context.Background()

	reader, err := objecttool.Open(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to open object: %w", err)
	}
	defer reader.Close()

	info := reader.Info()
	fmt.Printf("Object: %s\n", info.Path)
	fmt.Printf("Blocks: %d\n", info.BlockCount)
	fmt.Printf("Rows:   %d\n", info.RowCount)
	fmt.Printf("Cols:   %d\n", info.ColCount)

	fmt.Println("\nColumns:")
	cols := reader.Columns()
	for _, col := range cols {
		fmt.Printf("  %2d: %s\n", col.Idx, col.Type.String())
	}

	return nil
}
