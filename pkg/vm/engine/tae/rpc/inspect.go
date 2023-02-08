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
	"os"
	"sync"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/spf13/cobra"
)

type inspectContext struct {
	sync.Mutex
	db     *db.DB
	acinfo *db.AccessInfo
}

func (i *inspectContext) Init(db *db.DB, info *db.AccessInfo) {
	i.Lock()
	i.db = db
	i.acinfo = info
}

func (i *inspectContext) Destroy() {
	i.Unlock()
	i.db = nil
	i.acinfo = nil
}

var inspectCtx *inspectContext

var rootCmd = &cobra.Command{
	Use: "inspect",
}

type catalogArg struct {
	outfile *os.File
	verbose common.PPLevel
}

func (c *catalogArg) fromCommand(cmd *cobra.Command) error {
	count, _ := cmd.Flags().GetCount("verbose")
	var lv common.PPLevel
	switch count {
	case 0:
		lv = common.PPL0
	case 1:
		lv = common.PPL1
	case 2:
		lv = common.PPL2
	case 3:
		lv = common.PPL3
	}
	c.verbose = lv

	file, _ := cmd.Parent().PersistentFlags().GetString("outfile")
	if file != "" {
		if f, err := os.Create(file); err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("open %s err: %v", file, err)))
			return err
		} else {
			c.outfile = f
		}
	}
	return nil
}

func runCatalog(arg *catalogArg, respWriter io.Writer) {
	ret := inspectCtx.db.Catalog.SimplePPString(arg.verbose)

	if arg.outfile != nil {
		arg.outfile.WriteString(ret)
		defer arg.outfile.Close()
		respWriter.Write([]byte("write file done"))
	} else {
		respWriter.Write([]byte(ret))
	}
}

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "show catalog",
	Run: func(cmd *cobra.Command, args []string) {
		arg := &catalogArg{}
		if err := arg.fromCommand(cmd); err != nil {
			return
		}
		runCatalog(arg, cmd.OutOrStdout())
	},
}

func init() {
	inspectCtx = new(inspectContext)

	rootCmd.PersistentFlags().StringP("outfile", "o", "", "write output to a file")

	catalogCmd.Flags().CountP("verbose", "v", "verbose level")

	rootCmd.AddCommand(catalogCmd)
}

func RunInspect(arg string, out io.Writer) {
	args, _ := shlex.Split(arg)
	rootCmd.SetArgs(args)
	rootCmd.SetErr(out)
	rootCmd.SetOut(out)
	rootCmd.Execute()
}
