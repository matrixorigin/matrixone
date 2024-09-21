// Copyright 2023 Matrix Origin
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

package mo_debug

import (
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:        "debug",
		Short:      "MO debug tool",
		Long:       "MO debug tool. Helps to analyze MO problems like Goroutine, Txn, etc.",
		SuggestFor: []string{"mo-tool"},
		Version:    "0.1.0",
	}

	rootCmd.AddCommand(goroutineCMD)
	return rootCmd
}
