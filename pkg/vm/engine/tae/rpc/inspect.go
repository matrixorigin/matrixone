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
	"context"
	"fmt"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
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
	ctx       *inspectContext
	outfile   *os.File
	tblHandle handle.Relation
	verbose   common.PPLevel
}

func (c *catalogArg) fromCommand(cmd *cobra.Command) (err error) {
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

	db, _ := cmd.Flags().GetString("db")
	table, _ := cmd.Flags().GetString("table")
	if db != "" && table != "" {
		txn, _ := c.ctx.db.StartTxn(nil)
		dbHdl, err := txn.GetDatabase(db)
		if err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%v err: %v", db, err)))
			return err
		}
		tblHdl, err := dbHdl.GetRelationByName(table)
		if err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%v err: %v", table, err)))
			return err
		}
		c.tblHandle = tblHdl
	}
	file, _ := cmd.Flags().GetString("outfile")
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
		var ret string
		if arg.tblHandle != nil {
			meta := arg.tblHandle.GetMeta().(*catalog.TableEntry)
			ret = meta.PPString(arg.verbose, 0, "")
		} else {
			ret = arg.ctx.db.Catalog.SimplePPString(arg.verbose)
		}
		arg.outfile.WriteString(ret)
		defer arg.outfile.Close()
		respWriter.Write([]byte("write file done"))
	} else {
		var visitor *catalogRespVisitor
		if arg.tblHandle != nil {
			visitor = newTableRespVisitor(arg.verbose)
			meta := arg.tblHandle.GetMeta().(*catalog.TableEntry)
			meta.RecurLoop(visitor)
		} else {
			visitor = newCatalogRespVisitor(arg.verbose)
			arg.ctx.db.Catalog.RecurLoop(visitor)
		}
		ret, _ := types.Encode(visitor.GetResponse())
		arg.ctx.resp.Payload = ret
		arg.ctx.resp.Typ = db.InspectCata
	}
}

type addCCmd struct {
	ctx                *inspectContext
	db, tbl, name, typ string
	pos                int
}

func (c *addCCmd) fromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	c.db, _ = cmd.Flags().GetString("db")
	c.tbl, _ = cmd.Flags().GetString("table")
	c.name, _ = cmd.Flags().GetString("name")
	c.typ, _ = cmd.Flags().GetString("type")
	c.pos, _ = cmd.Flags().GetInt("pos")

	return nil
}

func runAddC(ctx context.Context, cmd *addCCmd) error {
	txn, _ := cmd.ctx.db.StartTxn(nil)
	db, _ := txn.GetDatabase(cmd.db)
	tbl, _ := db.GetRelationByName(cmd.tbl)
	typ, ok := types.Types[cmd.typ]
	planTyp := types.NewProtoType(typ.ToType().Oid)
	if !ok {
		planTyp = types.NewProtoType(types.T_uint32)
	}
	err := tbl.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, cmd.name, planTyp, int32(cmd.pos)))
	if err != nil {
		if txn.Rollback(ctx) != nil {
			panic("rollback error are you kidding?")
		}
		return err
	}
	return txn.Commit(ctx)
}

type dropCCmd struct {
	ctx     *inspectContext
	db, tbl string
	pos     int
}

func (c *dropCCmd) fromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	c.db, _ = cmd.Flags().GetString("db")
	c.tbl, _ = cmd.Flags().GetString("table")
	c.pos, _ = cmd.Flags().GetInt("pos")

	return nil
}

func runDropC(ctx context.Context, cmd *dropCCmd) error {
	txn, _ := cmd.ctx.db.StartTxn(nil)
	db, _ := txn.GetDatabase(cmd.db)
	tbl, _ := db.GetRelationByName(cmd.tbl)
	seqnum := tbl.Schema().(*catalog.Schema).ColDefs[cmd.pos].SeqNum
	err := tbl.AlterTable(context.TODO(), api.NewRemoveColumnReq(0, 0, uint32(cmd.pos), uint32(seqnum)))
	if err != nil {
		if txn.Rollback(ctx) != nil {
			panic("rollback error are you kidding?")
		}
		return err
	}
	return txn.Commit(ctx)
}

type renameTCmd struct {
	ctx          *inspectContext
	db, old, new string
}

func (c *renameTCmd) fromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	c.db, _ = cmd.Flags().GetString("db")
	c.old, _ = cmd.Flags().GetString("old")
	c.new, _ = cmd.Flags().GetString("new")

	return nil
}

func runRenameT(ctx context.Context, cmd *renameTCmd) error {
	txn, _ := cmd.ctx.db.StartTxn(nil)
	db, _ := txn.GetDatabase(cmd.db)
	tbl, _ := db.GetRelationByName(cmd.old)
	err := tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, cmd.old, cmd.new))
	if err != nil {
		if txn.Rollback(ctx) != nil {
			panic("rollback error are you kidding?")
		}
		return err
	}
	return txn.Commit(ctx)
}

func initCommand(ctx context.Context, inspectCtx *inspectContext) *cobra.Command {
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
	addCCmd := &cobra.Command{
		Use:   "addc",
		Short: "add column",
		Run: func(cmd *cobra.Command, args []string) {
			arg := &addCCmd{}
			if err := arg.fromCommand(cmd); err != nil {
				return
			}
			err := runAddC(ctx, arg)
			if err != nil {
				cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%v", err)))
			} else {
				cmd.OutOrStdout().Write([]byte("add column success"))
			}
		},
	}

	dropCCmd := &cobra.Command{
		Use:   "dropc",
		Short: "add column",
		Run: func(cmd *cobra.Command, args []string) {
			arg := &dropCCmd{}
			if err := arg.fromCommand(cmd); err != nil {
				return
			}
			err := runDropC(ctx, arg)
			if err != nil {
				cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%v", err)))
			} else {
				cmd.OutOrStdout().Write([]byte("drop column success"))
			}
		},
	}

	renameTCmd := &cobra.Command{
		Use:   "renamet",
		Short: "rename table",
		Run: func(cmd *cobra.Command, args []string) {
			arg := &renameTCmd{}
			if err := arg.fromCommand(cmd); err != nil {
				return
			}
			err := runRenameT(ctx, arg)
			if err != nil {
				cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%v", err)))
			} else {
				cmd.OutOrStdout().Write([]byte("rename table success"))
			}
		},
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.args)
	rootCmd.SetErr(inspectCtx.out)
	rootCmd.SetOut(inspectCtx.out)

	catalogCmd.Flags().CountP("verbose", "v", "verbose level")
	catalogCmd.Flags().StringP("outfile", "o", "", "write output to a file")
	catalogCmd.Flags().StringP("db", "d", "", "database name")
	catalogCmd.Flags().StringP("table", "t", "", "table name")
	rootCmd.AddCommand(catalogCmd)

	addCCmd.Flags().StringP("db", "d", "", "database")
	addCCmd.Flags().StringP("table", "b", "", "table")
	addCCmd.Flags().StringP("name", "n", "", "column name")
	addCCmd.Flags().StringP("type", "t", "", "type name")
	addCCmd.Flags().IntP("pos", "p", -1, "column postion")
	rootCmd.AddCommand(addCCmd)

	dropCCmd.Flags().StringP("db", "d", "", "database")
	dropCCmd.Flags().StringP("table", "b", "", "table")
	dropCCmd.Flags().IntP("pos", "p", -1, "column postion")
	rootCmd.AddCommand(dropCCmd)

	renameTCmd.Flags().StringP("db", "d", "", "database")
	renameTCmd.Flags().StringP("old", "o", "", "old table")
	renameTCmd.Flags().StringP("new", "n", "", "new table")
	rootCmd.AddCommand(renameTCmd)

	return rootCmd
}

func RunInspect(ctx context.Context, inspectCtx *inspectContext) {
	rootCmd := initCommand(ctx, inspectCtx)
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

func newTableRespVisitor(lv common.PPLevel) *catalogRespVisitor {
	v := &catalogRespVisitor{
		level: lv,
		stack: []*db.CatalogResp{{Item: "Catalog"}},
	}
	v.onstack(0, &db.CatalogResp{Item: "DB"})
	v.onstack(1, &db.CatalogResp{Item: "Tbl"})
	return v
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
