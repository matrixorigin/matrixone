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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
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

type InspectCmd interface {
	FromCommand(cmd *cobra.Command) error
	String() string
	Run() error
}

type catalogArg struct {
	ctx     *inspectContext
	outfile *os.File
	tbl     *catalog.TableEntry
	verbose common.PPLevel
}

func (c *catalogArg) FromCommand(cmd *cobra.Command) (err error) {
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

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}

	file, _ := cmd.Flags().GetString("outfile")
	if file != "" {
		if f, err := os.Create(file); err != nil {
			return moerr.NewInternalErrorNoCtx("open %s err: %v", file, err)
		} else {
			c.outfile = f
		}
	}
	return nil
}

func (c *catalogArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}
	f := "nil"
	if c.outfile != nil {
		f = c.outfile.Name()
	}
	return fmt.Sprintf("(%s) outfile: %v, verbose: %d, ", t, f, c.verbose)
}

func (c *catalogArg) Run() error {
	var ret string
	if c.tbl != nil {
		ret = c.tbl.PPString(c.verbose, 0, "")
	} else {
		ret = c.ctx.db.Catalog.SimplePPString(c.verbose)
	}
	if c.outfile != nil {
		c.outfile.WriteString(ret)
		defer c.outfile.Close()
		c.ctx.resp.Payload = []byte("write file done")
	} else {
		c.ctx.resp.Payload = []byte(ret)
	}
	return nil
}

type objStatArg struct {
	ctx     *inspectContext
	tbl     *catalog.TableEntry
	verbose common.PPLevel
}

func (c *objStatArg) FromCommand(cmd *cobra.Command) (err error) {
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
	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}
	return nil
}

func (c *objStatArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}
	return t
}

func (c *objStatArg) Run() error {
	if c.tbl != nil {
		b := &bytes.Buffer{}
		p := c.ctx.db.MergeHandle.GetPolicy(c.tbl.ID).(*merge.BasicPolicyConfig)
		b.WriteString(c.tbl.ObjectStatsString(c.verbose))
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf("\n%s", p.String()))
		c.ctx.resp.Payload = b.Bytes()
	}
	return nil
}

type manuallyMergeArg struct {
	ctx     *inspectContext
	tbl     *catalog.TableEntry
	objects []*catalog.SegmentEntry
}

func (c *manuallyMergeArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}

	objects, _ := cmd.Flags().GetStringSlice("objects")

	dedup := make(map[string]struct{}, len(objects))
	for _, o := range objects {
		if _, ok := dedup[o]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate object %s", o)
		}
		dedup[o] = struct{}{}
	}
	if len(dedup) < 2 {
		return moerr.NewInvalidInputNoCtx("need at least 2 objects")
	}
	segs := make([]*catalog.SegmentEntry, 0, len(objects))
	for o := range dedup {
		parts := strings.Split(o, "_")
		uid, err := types.ParseUuid(parts[0])
		if err != nil {
			return err
		}
		seg, err := c.tbl.GetSegmentByID(&uid)
		if err != nil {
			return moerr.NewInvalidInputNoCtx("not found object %s", o)
		}
		if !seg.IsActive() || !seg.IsSorted() || seg.GetNextObjectIndex() != 1 {
			return moerr.NewInvalidInputNoCtx("object is deleted or not a flushed one %s", o)
		}
		segs = append(segs, seg)
	}

	c.objects = segs
	return nil
}

func (c *manuallyMergeArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}

	b := &bytes.Buffer{}
	for _, o := range c.objects {
		b.WriteString(fmt.Sprintf("%s_0000,", o.ID.ToString()))
	}

	return fmt.Sprintf("(%s) objects: %s", t, b.String())
}

func (c *manuallyMergeArg) Run() error {
	err := c.ctx.db.MergeHandle.ManuallyMerge(c.tbl, c.objects)
	if err != nil {
		return err
	}
	c.ctx.resp.Payload = []byte(fmt.Sprintf(
		"success. see more to run select mo_ctl('dn', 'inspect', 'object -t %s.%s')\\G",
		c.tbl.GetDB().GetName(), c.tbl.GetLastestSchema().Name,
	))
	return nil
}

type compactPolicyArg struct {
	ctx              *inspectContext
	tbl              *catalog.TableEntry
	maxMergeObjN     int32
	minRowsQualified int32
	notLoadMoreThan  int32
}

func (c *compactPolicyArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	c.maxMergeObjN, _ = cmd.Flags().GetInt32("maxMergeObjN")
	c.minRowsQualified, _ = cmd.Flags().GetInt32("minRowsQualified")
	c.notLoadMoreThan, _ = cmd.Flags().GetInt32("notLoadMoreThan")
	return nil
}

func (c *compactPolicyArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}
	return fmt.Sprintf(
		"(%s) maxMergeObjN: %v, minRowsQualified: %v, notLoadMoreThan: %v",
		t, c.maxMergeObjN, c.minRowsQualified, c.notLoadMoreThan,
	)
}

func (c *compactPolicyArg) Run() error {
	if c.tbl == nil {
		// reset all
		c.ctx.db.MergeHandle.ConfigPolicy(0, nil)
		// set runtime global config
		common.RuntimeMaxMergeObjN.Store(c.maxMergeObjN)
		common.RuntimeMinRowsQualified.Store(c.minRowsQualified)
	} else {
		c.ctx.db.MergeHandle.ConfigPolicy(c.tbl.ID, &merge.BasicPolicyConfig{
			MergeMaxOneRun: int(c.maxMergeObjN),
			ObjectMinRows:  int(c.minRowsQualified),
		})
	}
	common.RuntimeNotLoadMoreThan.Store(c.notLoadMoreThan)
	c.ctx.resp.Payload = []byte("<empty>")
	return nil
}

func RunFactory[T InspectCmd](t T) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := t.FromCommand(cmd); err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("parse err: %v", err)))
			return
		}
		err := t.Run()
		if err != nil {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("run err: %v", err)),
			)
		} else {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("success. arg %v", t.String())),
			)
		}
	}
}

func initCommand(ctx context.Context, inspectCtx *inspectContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "inspect",
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.args)
	rootCmd.SetErr(inspectCtx.out)
	rootCmd.SetOut(inspectCtx.out)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	catalogCmd := &cobra.Command{
		Use:   "catalog",
		Short: "show catalog",
		Run:   RunFactory(&catalogArg{}),
	}

	catalogCmd.Flags().CountP("verbose", "v", "verbose level")
	catalogCmd.Flags().StringP("outfile", "o", "", "write output to a file")
	catalogCmd.Flags().StringP("target", "t", "*", "format: db.table")
	rootCmd.AddCommand(catalogCmd)

	objectCmd := &cobra.Command{
		Use:   "object",
		Short: "show object statistics",
		Run:   RunFactory(&objStatArg{}),
	}
	objectCmd.Flags().CountP("verbose", "v", "verbose level")
	objectCmd.Flags().StringP("target", "t", "*", "format: db.table")
	rootCmd.AddCommand(objectCmd)

	policyCmd := &cobra.Command{
		Use:   "policy",
		Short: "set merge policy for table",
		Run:   RunFactory(&compactPolicyArg{}),
	}
	policyCmd.Flags().StringP("target", "t", "*", "format: db.table")
	policyCmd.Flags().Int32P("maxMergeObjN", "o", common.DefaultMaxMergeObjN, "max number of objects merged for one")
	policyCmd.Flags().Int32P("minRowsQualified", "m", common.DefaultMinRowsQualified, "object with a few rows will be picked up to merge")
	policyCmd.Flags().Int32P("notLoadMoreThan", "l", common.DefaultNotLoadMoreThan, "not load metadata if table has too much objects. Only works for rawlog table")
	rootCmd.AddCommand(policyCmd)

	mmCmd := &cobra.Command{
		Use:   "merge",
		Short: "manually merge objects",
		Run:   RunFactory(&manuallyMergeArg{}),
	}

	mmCmd.Flags().StringP("target", "t", "*", "format: db.table")
	mmCmd.Flags().StringSliceP("objects", "o", nil, "format: object_id_0000,object_id_0000")
	rootCmd.AddCommand(mmCmd)

	return rootCmd
}

func RunInspect(ctx context.Context, inspectCtx *inspectContext) {
	rootCmd := initCommand(ctx, inspectCtx)
	rootCmd.Execute()
}

func parseTableTarget(address string, ac *db.AccessInfo, db *db.DB) (*catalog.TableEntry, error) {
	if address == "*" {
		return nil, nil
	}
	parts := strings.Split(address, ".")
	if len(parts) != 2 {
		return nil, moerr.NewInvalidInputNoCtx(fmt.Sprintf("invalid db.table: %q", address))
	}

	txn, _ := db.StartTxn(nil)
	if ac != nil {
		logutil.Infof("inspect with access info: %+v", ac)
		txn.BindAccessInfo(ac.AccountID, ac.UserID, ac.RoleID)
	}

	did, err1 := strconv.Atoi(parts[0])
	tid, err2 := strconv.Atoi(parts[1])

	if err1 == nil && err2 == nil {
		dbHdl, err := txn.GetDatabaseByID(uint64(did))
		if err != nil {
			return nil, err
		}
		tblHdl, err := dbHdl.GetRelationByID(uint64(tid))
		if err != nil {
			return nil, err
		}
		txn.Commit(context.Background())
		return tblHdl.GetMeta().(*catalog.TableEntry), nil
	} else {
		dbHdl, err := txn.GetDatabase(parts[0])
		if err != nil {
			return nil, err
		}
		tblHdl, err := dbHdl.GetRelationByName(parts[1])
		if err != nil {
			return nil, err
		}
		txn.Commit(context.Background())
		return tblHdl.GetMeta().(*catalog.TableEntry), nil
	}
}
