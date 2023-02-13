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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/spf13/cobra"
)

type inspectContext struct {
	db     *db.DB
	acinfo *db.AccessInfo
	args   []string
	out    io.Writer
	resp   *db.InspectResp
}

// impl Pflag.Value interface
func (i *inspectContext) String() string   { return "" }
func (i *inspectContext) Set(string) error { return nil }
func (i *inspectContext) Type() string     { return "ictx" }

type catalogArg struct {
	ctx     *inspectContext
	outfile *os.File
	verbose common.PPLevel
}

func (c *catalogArg) fromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
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
	if arg.outfile != nil {
		ret := arg.ctx.db.Catalog.SimplePPString(arg.verbose)
		arg.outfile.WriteString(ret)
		defer arg.outfile.Close()
		respWriter.Write([]byte("write file done"))
	} else {
		visitor := newCatalogRespVisitor(arg.verbose)
		arg.ctx.db.Catalog.RecurLoop(visitor)

		ret, _ := types.Encode(visitor.GetResponse())
		arg.ctx.resp.Payload = ret
		arg.ctx.resp.Typ = db.InspectCata
	}
}

func initCommand(ctx *inspectContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "inspect",
	}

	catalogCmd := &cobra.Command{
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

	rootCmd.PersistentFlags().VarPF(ctx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(ctx.args)
	rootCmd.SetErr(ctx.out)
	rootCmd.SetOut(ctx.out)

	catalogCmd.Flags().CountP("verbose", "v", "verbose level")
	catalogCmd.Flags().StringP("outfile", "o", "", "write output to a file")
	rootCmd.AddCommand(catalogCmd)
	return rootCmd
}

func RunInspect(ctx *inspectContext) {
	rootCmd := initCommand(ctx)
	rootCmd.Execute()
}

type catalogRespVisitor struct {
	catalog.LoopProcessor
	level common.PPLevel
	stack []*db.CatalogResp
}

func newCatalogRespVisitor(lv common.PPLevel) *catalogRespVisitor {
	return &catalogRespVisitor{
		level: lv,
		stack: []*db.CatalogResp{{Item: "Catalog"}},
	}
}

func (c *catalogRespVisitor) GetResponse() *db.CatalogResp {
	return c.stack[0]
}

func entryLevelString[T interface {
	StringWithLevel(common.PPLevel) string
}](entry T, lv common.PPLevel) *db.CatalogResp {
	return &db.CatalogResp{Item: entry.StringWithLevel(lv)}
}

func (c *catalogRespVisitor) onstack(idx int, resp *db.CatalogResp) {
	c.stack = c.stack[:idx+1]
	c.stack[idx].Sub = append(c.stack[idx].Sub, resp)
	c.stack = append(c.stack, resp)
}

func (c *catalogRespVisitor) OnDatabase(database *catalog.DBEntry) error {
	c.onstack(0, entryLevelString(database, c.level))
	return nil
}

func (c *catalogRespVisitor) OnTable(table *catalog.TableEntry) error {
	if c.level == common.PPL0 {
		return moerr.GetOkStopCurrRecur()
	}
	c.onstack(1, entryLevelString(table, c.level))
	return nil
}

func (c *catalogRespVisitor) OnSegment(seg *catalog.SegmentEntry) error {
	if c.level == common.PPL0 {
		return moerr.GetOkStopCurrRecur()
	}
	c.onstack(2, entryLevelString(seg, c.level))
	return nil
}

func (c *catalogRespVisitor) OnBlock(blk *catalog.BlockEntry) error {
	if c.level == common.PPL0 {
		return moerr.GetOkStopCurrRecur()
	}
	c.onstack(3, entryLevelString(blk, c.level))
	return nil
}
