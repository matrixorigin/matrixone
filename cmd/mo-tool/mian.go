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

package main

import (
	debug "github.com/matrixorigin/matrixone/cmd/mo-debug"
	inspect "github.com/matrixorigin/matrixone/cmd/mo-inspect"
	"github.com/spf13/cobra"
	"os"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "mo-tool",
		Short: "Mo tool",
		Long:  "Mo tool is a multifunctional development tool",
	}

	rootCmd.AddCommand(debug.PrepareCommand())
	rootCmd.AddCommand(inspect.PrepareCommand())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
