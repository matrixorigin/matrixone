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
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
	"github.com/spf13/cobra"
)

func viewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "view <object-file>",
		Short: "Interactive object file viewer",
		Long:  "Open an object file in interactive mode for browsing and analysis",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			return interactive.Run(path)
		},
	}

	return cmd
}
