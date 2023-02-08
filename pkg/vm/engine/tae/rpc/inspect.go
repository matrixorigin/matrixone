// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"fmt"
	"io"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "inspect",
}

type catalogArg struct {
	verbose int
}

func runCatalog(arg *catalogArg) {
	var lv common.PPLevel
	switch arg.verbose {
	case 0:
		lv = common.PPL0
	case 1:
		lv = common.PPL1
	case 2:
		lv = common.PPL2
	case 3:
		lv = common.PPL3
	}
}

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "show catalog",
	Run: func(cmd *cobra.Command, args []string) {
		count, _ := cmd.Flags().GetCount("verbose")
		arg := &catalogArg{
			verbose: count,
		}
		cmd.OutOrStderr().Write([]byte(fmt.Sprintf("xxxxxxx %#v  %#v %d\n", cmd, args, count)))
	},
}

func init() {
	catalogCmd.Flags().CountP("verbose", "v", "verbose level")
	rootCmd.AddCommand(catalogCmd)
}

func RunInspect(arg string, out io.Writer, *db.DB) {
	args, _ := shlex.Split(arg)
	rootCmd.SetArgs(args)
	rootCmd.SetErr(out)
	rootCmd.SetOut(out)
	rootCmd.Execute()
}
