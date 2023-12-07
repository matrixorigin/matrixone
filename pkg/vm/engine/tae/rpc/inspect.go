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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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
		p := c.ctx.db.MergeHandle.GetPolicy(c.tbl).(*merge.BasicPolicyConfig)
		b.WriteString(c.tbl.ObjectStatsString(c.verbose))
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf("\n%s", p.String()))
		c.ctx.resp.Payload = b.Bytes()
	}
	return nil
}

type manualyIgnoreArg struct {
	ctx *inspectContext
	id  uint64
}

func (c *manualyIgnoreArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	c.id, _ = cmd.Flags().GetUint64("tid")
	return nil
}

func (c *manualyIgnoreArg) String() string {
	return fmt.Sprintf("ignore ckp table: %v", c.id)
}

func (c *manualyIgnoreArg) Run() error {
	logtail.TempF.Add(c.id)
	return nil
}

type infoArg struct {
	ctx *inspectContext
	tbl *catalog.TableEntry
	blk *catalog.BlockEntry
}

func (c *infoArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}

	baddress, _ := cmd.Flags().GetString("blk")
	c.blk, err = parseBlkTarget(baddress, c.tbl)
	if err != nil {
		return err
	}

	return nil
}

func (c *infoArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}

	if c.blk != nil {
		t = fmt.Sprintf("%s %s", t, c.blk.ID.String())
	}

	return fmt.Sprintf("info: %v", t)
}

func (c *infoArg) Run() error {
	b := &bytes.Buffer{}
	if c.tbl != nil {
		b.WriteString(fmt.Sprintf("last_merge: %v\n", c.tbl.Stats.GetLastMerge().String()))
		b.WriteString(fmt.Sprintf("last_flush: %v\n", c.tbl.Stats.GetLastFlush().ToString()))
	}
	if c.blk != nil {
		b.WriteRune('\n')
		b.WriteString(fmt.Sprintf("persisted_ts: %v\n", c.blk.GetDeltaPersistedTS().ToString()))
		b.WriteString(fmt.Sprintf("delchain: %v\n", c.blk.GetBlockData().DeletesInfo()))
	}
	c.ctx.resp.Payload = b.Bytes()
	return nil
}

type manuallyMergeArg struct {
	ctx     *inspectContext
	tbl     *catalog.TableEntry
	objects []*catalog.ObjectEntry
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
	objs := make([]*catalog.ObjectEntry, 0, len(objects))
	for o := range dedup {
		parts := strings.Split(o, "_")
		uid, err := types.ParseUuid(parts[0])
		if err != nil {
			return err
		}
		objects, err := c.tbl.GetObjectsByID(&uid)
		if err != nil {
			return moerr.NewInvalidInputNoCtx("not found object %s", o)
		}
		for _, obj := range objects {
			if !obj.IsActive() || obj.IsAppendable() || obj.GetNextObjectIndex() != 1 {
				return moerr.NewInvalidInputNoCtx("object is deleted or not a flushed one %s", o)
			}
			objs = append(objs, obj)
		}
	}

	c.objects = objs
	return nil
}

func (c *manuallyMergeArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}

	b := &bytes.Buffer{}
	for _, o := range c.objects {
		b.WriteString(fmt.Sprintf("%s_0000,", o.ID.String()))
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
	hints            []api.MergeHint
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
	hints, _ := cmd.Flags().GetInt32Slice("mergeHints")
	for _, h := range hints {
		if _, ok := api.MergeHint_name[h]; !ok {
			return moerr.NewInvalidArgNoCtx("unspported hint %v", h)
		}
		c.hints = append(c.hints, api.MergeHint(h))
	}
	return nil
}

func (c *compactPolicyArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchema().Name)
	}
	return fmt.Sprintf(
		"(%s) maxMergeObjN: %v, minRowsQualified: %v, hints: %v",
		t, c.maxMergeObjN, c.minRowsQualified, c.hints,
	)
}

func (c *compactPolicyArg) Run() error {
	if c.tbl == nil {
		common.RuntimeMaxMergeObjN.Store(c.maxMergeObjN)
		common.RuntimeMinRowsQualified.Store(c.minRowsQualified)
		if c.maxMergeObjN == 0 && c.minRowsQualified == 0 {
			merge.StopMerge.Store(true)
		} else {
			merge.StopMerge.Store(false)
		}
	} else {
		c.ctx.db.MergeHandle.ConfigPolicy(c.tbl, &merge.BasicPolicyConfig{
			MergeMaxOneRun: int(c.maxMergeObjN),
			ObjectMinRows:  int(c.minRowsQualified),
			MergeHints:     c.hints,
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
	policyCmd.Flags().Int32P("maxMergeObjN", "r", common.DefaultMaxMergeObjN, "max number of objects merged for one run")
	policyCmd.Flags().Int32P("minRowsQualified", "m", common.DefaultMinRowsQualified, "objects which are less than minRowsQualified will be picked up to merge")
	policyCmd.Flags().Int32P("notLoadMoreThan", "l", common.DefaultNotLoadMoreThan, "not load metadata if table has too much objects. Only works for rawlog table")
	policyCmd.Flags().Int32SliceP("mergeHints", "n", []int32{0}, "hints to merge the table")
	policyCmd.Flags().MarkHidden("notLoadMoreThan")
	rootCmd.AddCommand(policyCmd)

	mmCmd := &cobra.Command{
		Use:   "merge",
		Short: "manually merge objects",
		Run:   RunFactory(&manuallyMergeArg{}),
	}

	mmCmd.Flags().StringP("target", "t", "*", "format: db.table")
	mmCmd.Flags().StringSliceP("objects", "o", nil, "format: object_id_0000,object_id_0000")
	rootCmd.AddCommand(mmCmd)

	infoCmd := &cobra.Command{
		Use:   "info",
		Short: "get dedicated debug info",
		Run:   RunFactory(&infoArg{}),
	}

	infoCmd.Flags().StringP("target", "t", "*", "format: table-id")
	infoCmd.Flags().StringP("blk", "b", "", "format: <objectId>_<fineN>_<blkN>")

	rootCmd.AddCommand(infoCmd)

	miCmd := &cobra.Command{
		Use:   "ckpignore",
		Short: "manually ignore table when checking checkpoint entry",
		Run:   RunFactory(&manualyIgnoreArg{}),
	}

	miCmd.Flags().Uint64P("tid", "t", 0, "format: table-id")
	rootCmd.AddCommand(miCmd)

	return rootCmd
}

func RunInspect(ctx context.Context, inspectCtx *inspectContext) {
	rootCmd := initCommand(ctx, inspectCtx)
	rootCmd.Execute()
}

func parseBlkTarget(address string, tbl *catalog.TableEntry) (*catalog.BlockEntry, error) {
	if address == "" {
		return nil, nil
	}
	parts := strings.Split(address, "_")
	if len(parts) != 3 {
		return nil, moerr.NewInvalidInputNoCtx(fmt.Sprintf("invalid db.table: %q", address))
	}
	uid, err := types.ParseUuid(parts[0])
	if err != nil {
		return nil, err
	}
	fn, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	bn, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	bid := objectio.NewBlockid(&uid, uint16(fn), uint16(bn))
	objid := bid.Object()
	sentry, err := tbl.GetObjectByID(objid)
	if err != nil {
		return nil, err
	}
	bentry, err := sentry.GetBlockEntryByID(bid)
	if err != nil {
		return nil, err
	}
	return bentry, nil
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
		tbl := tblHdl.GetMeta().(*catalog.TableEntry)
		txn.Commit(context.Background())
		return tbl, nil
	} else {
		dbHdl, err := txn.GetDatabase(parts[0])
		if err != nil {
			return nil, err
		}
		tblHdl, err := dbHdl.GetRelationByName(parts[1])
		if err != nil {
			return nil, err
		}
		tbl := tblHdl.GetMeta().(*catalog.TableEntry)
		txn.Commit(context.Background())
		return tbl, nil
	}
}
