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
	"container/heap"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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

func initCommand(_ context.Context, inspectCtx *inspectContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "inspect",
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.args)
	rootCmd.SetErr(inspectCtx.out)
	rootCmd.SetOut(inspectCtx.out)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	catalog := &catalogArg{}
	rootCmd.AddCommand(catalog.PrepareCommand())

	object := &objStatArg{}
	rootCmd.AddCommand(object.PrepareCommand())

	policy := &mergePolicyArg{}
	rootCmd.AddCommand(policy.PrepareCommand())

	info := &infoArg{}
	rootCmd.AddCommand(info.PrepareCommand())

	mignore := &manualyIgnoreArg{}
	rootCmd.AddCommand(mignore.PrepareCommand())

	storage := &storageUsageHistoryArg{}
	rootCmd.AddCommand(storage.PrepareCommand())

	renamecol := &RenameColArg{}
	rootCmd.AddCommand(renamecol.PrepareCommand())

	pstatus := &PolicyStatus{}
	rootCmd.AddCommand(pstatus.PrepareCommand())

	objPrune := &objectPruneArg{}
	rootCmd.AddCommand(objPrune.PrepareCommand())

	transfer := &transferArg{}
	rootCmd.AddCommand(transfer.PrepareCommand())

	inspect := &MoInspectArg{}
	rootCmd.AddCommand(inspect.PrepareCommand())

	return rootCmd
}

func RunInspect(ctx context.Context, inspectCtx *inspectContext) {
	rootCmd := initCommand(ctx, inspectCtx)
	rootCmd.Execute()
}

type InspectCmd interface {
	FromCommand(cmd *cobra.Command) error
	String() string
	Run() error
}

func RunFactory[T InspectCmd](t T) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := t.FromCommand(cmd); err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("parse err: %v", err)))
			return
		}
		v := cmd.Flag("ictx")
		if v != nil {
			ctx := cmd.Flag("ictx").Value.(*inspectContext)
			logutil.Infof("inpsect mo_ctl %s: %v by account %+v", cmd.Name(), t.String(), ctx.acinfo)
		}
		err := t.Run()
		if err != nil {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("run err: %v", err)),
			)
		} else {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("%v", t.String())),
			)
		}
	}
}

type catalogArg struct {
	ctx     *inspectContext
	outfile *os.File
	tbl     *catalog.TableEntry
	verbose common.PPLevel
}

func (c *catalogArg) PrepareCommand() *cobra.Command {
	catalogCmd := &cobra.Command{
		Use:   "catalog",
		Short: "show catalog",
		Run:   RunFactory(c),
	}

	catalogCmd.Flags().CountP("verbose", "v", "verbose level")
	catalogCmd.Flags().StringP("outfile", "o", "", "write output to a file")
	catalogCmd.Flags().StringP("target", "t", "*", "format: db.table")
	return catalogCmd
}

func switchPPL(count int) common.PPLevel {
	switch count {
	case 0:
		return common.PPL0
	case 1:
		return common.PPL1
	case 2:
		return common.PPL2
	case 3:
		return common.PPL3
	case 4:
		return common.PPL4
	default:
		return common.PPL1
	}
}

func (c *catalogArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	count, _ := cmd.Flags().GetCount("verbose")
	c.verbose = switchPPL(count)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}

	file, _ := cmd.Flags().GetString("outfile")
	if file != "" {
		if f, err := os.Create(file); err != nil {
			return moerr.NewInternalErrorNoCtxf("open %s err: %v", file, err)
		} else {
			c.outfile = f
		}
	}
	return nil
}

func (c *catalogArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchemaLocked(false).Name)
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
	ctx              *inspectContext
	tbl              *catalog.TableEntry
	topk, start, end int
	verbose          common.PPLevel
}

func (c *objStatArg) PrepareCommand() *cobra.Command {
	objectCmd := &cobra.Command{
		Use:   "object",
		Short: "show object statistics",
		Run:   RunFactory(c),
	}
	objectCmd.Flags().CountP("verbose", "v", "verbose level")
	objectCmd.Flags().IntP("topk", "k", 10, "tables with topk objects count")
	objectCmd.Flags().StringP("target", "t", "*", "format: db.table")
	objectCmd.Flags().IntP("start", "s", 0, "show object detail starts from")
	objectCmd.Flags().IntP("end", "e", -1, "show object detail ends at")
	return objectCmd
}

func (c *objStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	c.topk, _ = cmd.Flags().GetInt("topk")
	c.start, _ = cmd.Flags().GetInt("start")
	c.end, _ = cmd.Flags().GetInt("end")
	count, _ := cmd.Flags().GetCount("verbose")
	c.verbose = switchPPL(count)
	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	return nil
}

func (c *objStatArg) String() string {
	if c.tbl != nil {
		return fmt.Sprintf("%d-%s verbose %v", c.tbl.ID, c.tbl.GetLastestSchema(false).Name, c.verbose)
	} else {
		return fmt.Sprintf("list with top %d", c.topk)
	}
}

func (c *objStatArg) Run() error {
	if c.tbl != nil {
		b := &bytes.Buffer{}
		p := c.ctx.db.MergeScheduler.GetPolicy(c.tbl)
		b.WriteString(c.tbl.ObjectStatsString(c.verbose, c.start, c.end, false))
		b.WriteByte('\n')
		b.WriteByte('\n')
		b.WriteString(c.tbl.ObjectStatsString(c.verbose, c.start, c.end, true))
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf("\n%s", p.String()))
		c.ctx.resp.Payload = b.Bytes()
	} else {
		visitor := newObjectVisitor()
		visitor.topk = c.topk
		c.ctx.db.Catalog.RecurLoop(visitor)
		b := &bytes.Buffer{}
		b.WriteString(fmt.Sprintf("db count: %d, table count: %d\n", visitor.db, visitor.tbl))
		for i, l := 0, visitor.candidates.Len(); i < l; i++ {
			item := heap.Pop(&visitor.candidates).(mItem)
			b.WriteString(fmt.Sprintf("  %d.%d: %d\n", item.did, item.tid, item.objcnt))
		}
		c.ctx.resp.Payload = b.Bytes()
	}
	return nil
}

type pruneTask struct {
	objs     []*catalog.ObjectEntry
	insertAt time.Time
}

type objsPruneTask struct {
	sync.Mutex
	memos map[int]pruneTask
}

func (c *objsPruneTask) PruneLocked() {
	for id, task := range c.memos {
		if time.Since(task.insertAt) > 5*time.Minute {
			delete(c.memos, id)
		}
	}
}

func (c *objsPruneTask) Len() int {
	c.Lock()
	defer c.Unlock()
	return len(c.memos)
}

var TaskCache = &objsPruneTask{
	memos: make(map[int]pruneTask),
}

type objectPruneArg struct {
	ctx   *inspectContext
	tbl   *catalog.TableEntry
	ago   time.Duration
	force bool // dedicated for prune_log
	ack   int
}

func (c *objectPruneArg) PrepareCommand() *cobra.Command {
	objectPruneCmd := &cobra.Command{
		Use:   "objprune",
		Short: "prune objects",
		Run:   RunFactory(c),
	}
	objectPruneCmd.Flags().StringP("target", "t", "*", "format: db.table")
	objectPruneCmd.Flags().DurationP("duration", "d", 72*time.Hour, "prune objects older than duration")
	objectPruneCmd.Flags().IntP("ack", "a", -1, "execute task by ack")
	objectPruneCmd.Flags().BoolP("force", "f", false, "force to prune")
	objectPruneCmd.Flags().MarkHidden("force")
	return objectPruneCmd
}

func (c *objectPruneArg) String() string {
	if c.ack != -1 {
		return fmt.Sprintf("prune: execute task: %d", c.ack)
	} else {
		return fmt.Sprintf("prune: table %v-%v, %v ago, cacheLen %v", c.tbl.ID, c.tbl.GetLastestSchema(false).Name, c.ago, TaskCache.Len())
	}
}

const AllowPruneDuration = 24 * time.Hour

func (c *objectPruneArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	address, _ := cmd.Flags().GetString("target")
	c.ack, _ = cmd.Flags().GetInt("ack")
	c.ago, _ = cmd.Flags().GetDuration("duration")
	c.force, _ = cmd.Flags().GetBool("force")
	if c.ago < AllowPruneDuration {
		return moerr.NewInvalidInputNoCtx("pruning objects within 24h is not supported")
	}
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.ack == -1 && c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}
	return nil
}

func (c *objectPruneArg) Run() error {
	if c.ack != -1 {
		TaskCache.Lock()
		task, ok := TaskCache.memos[c.ack]
		delete(TaskCache.memos, c.ack)
		TaskCache.Unlock()
		if !ok {
			c.ctx.resp.Payload = []byte("task not found")
			return nil
		}
		if err := c.executePrune(task.objs); err != nil {
			return err
		}
		return nil
	}

	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}

	TaskCache.Lock()
	TaskCache.PruneLocked()
	if len(TaskCache.memos) >= 100 {
		TaskCache.Unlock()
		return moerr.NewInvalidInputNoCtx("too many cache, try later")
	}
	TaskCache.Unlock()
	entry := c.tbl

	it := entry.MakeDataObjectIt()
	now := c.ctx.db.TxnMgr.Now()
	var total, stale, selected int
	var minR, maxR, totalR, minS, maxS, totalS int
	ago := types.BuildTS(now.Physical()-int64(c.ago), now.Logical())

	selectedObjs := make([]*catalog.ObjectEntry, 0, 64)

	for it.Next() {
		obj := it.Item()
		if obj.ObjectState != catalog.ObjectState_Create_ApplyCommit {
			continue
		}
		if !obj.IsActive() || obj.IsAppendable() {
			continue
		}
		total++

		createTs := obj.GetCreatedAt()
		if createTs.GE(&ago) {
			continue
		}
		stale++
		selected++
		selectedObjs = append(selectedObjs, obj)
		rw := int(obj.Rows())
		sz := int(obj.OriginSize())
		if minR == 0 || rw < minR {
			minR = rw
		}
		if rw > maxR {
			maxR = rw
		}
		totalR += rw
		if minS == 0 || sz < minS {
			minS = sz
		}
		if sz > maxS {
			maxS = sz
		}
		totalS += sz
	}
	it.Release()

	if selected == 0 {
		c.ctx.resp.Payload = []byte(fmt.Sprintf(
			"total: %d, stale: %d, selected: %d, no valid objs to prune",
			total, stale, selected,
		))
		return nil
	}
	id := -1
	if c.force {
		if err := c.executePrune(selectedObjs); err != nil {
			return err
		}
	} else {
		TaskCache.Lock()
		for {
			id = rand.Intn(100)
			if _, ok := TaskCache.memos[id]; ok {
				continue
			}
			TaskCache.memos[id] = pruneTask{
				objs:     selectedObjs,
				insertAt: time.Now(),
			}
			break
		}
		TaskCache.Unlock()
	}

	pad := ""
	if len(c.ctx.resp.Payload) > 0 {
		pad = "\n"
	}
	c.ctx.resp.Payload = append(c.ctx.resp.Payload, []byte(fmt.Sprintf(
		"%vtotal: %d, stale: %d, selected: %d, minR: %d, maxR: %d, avgR: %d, minS: %v, maxS: %v, avgS: %v, taskid: %d",
		pad,
		total, stale, selected, minR, maxR, totalR/selected,
		common.HumanReadableBytes(minS),
		common.HumanReadableBytes(maxS),
		common.HumanReadableBytes(totalS/selected),
		id,
	))...)

	return nil
}

func (c *objectPruneArg) executePrune(objs []*catalog.ObjectEntry) error {
	txn, _ := c.ctx.db.StartTxn(nil)
	tid := objs[0].GetTable().ID
	did := objs[0].GetTable().GetDB().ID
	dbHdl, err := txn.GetDatabaseByID(uint64(did))
	if err != nil {
		return err
	}
	tblHdl, err := dbHdl.GetRelationByID(uint64(tid))
	if err != nil {
		return err
	}
	notfound := 0
	w := &bytes.Buffer{}
	for _, obj := range objs {
		if err := tblHdl.SoftDeleteObject(obj.ID(), obj.IsTombstone); err != nil {
			logutil.Errorf("objprune: del obj %s: %v", obj.ID().String(), err)
			return err
		}
		w.WriteString(obj.ID().String())
		w.WriteRune(',')
	}
	if err := txn.Commit(context.Background()); err != nil {
		return err
	}

	logutil.Infof("objprune done: %v", w.String())
	pad := ""
	if len(c.ctx.resp.Payload) > 0 {
		pad = "\n"
	}
	c.ctx.resp.Payload = append(c.ctx.resp.Payload,
		[]byte(fmt.Sprintf("%vdone. prunes total: %d, notfound: %d", pad, len(objs), notfound))...)
	return nil
}

type storageUsageHistoryArg struct {
	ctx    *inspectContext
	detail *struct {
		accId uint64
		dbI   uint64
		tblId uint64
	}

	trace *struct {
		tStart, tEnd time.Time
		accounts     map[uint64]struct{}
	}

	transfer        bool
	eliminateErrors bool
}

func (c *storageUsageHistoryArg) PrepareCommand() *cobra.Command {
	storageUsageCmd := &cobra.Command{
		Use:   "storage_usage",
		Short: "storage usage details",
		Run:   RunFactory(c),
	}

	// storage usage request history
	storageUsageCmd.Flags().StringP("trace", "t", "", "format: -time time range or -acc account id list")
	// storage usage details in ckp
	storageUsageCmd.Flags().StringP("detail", "d", "", "format: accId{.dbName{.tableName}}")
	storageUsageCmd.Flags().StringP("transfer", "f", "", "format: *")
	storageUsageCmd.Flags().StringP("eliminate_errors", "e", "", "format: *")
	return storageUsageCmd
}

func (c *storageUsageHistoryArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	expr, _ := cmd.Flags().GetString("detail")
	if expr != "" {
		accId, dbId, tblId, err := parseStorageUsageDetail(expr, c.ctx.acinfo, c.ctx.db)
		if err != nil {
			return err
		}
		c.detail = &struct {
			accId uint64
			dbI   uint64
			tblId uint64
		}{accId: accId, dbI: dbId, tblId: tblId}
	}

	expr, _ = cmd.Flags().GetString("trace")
	if expr != "" {
		start, end, accs, err := parseStorageUsageTrace(expr, c.ctx.acinfo, c.ctx.db)
		if err != nil {
			return err
		}

		c.trace = &struct {
			tStart, tEnd time.Time
			accounts     map[uint64]struct{}
		}{tStart: start, tEnd: end, accounts: accs}
	}

	expr, _ = cmd.Flags().GetString("transfer")
	if expr != "" {
		if expr == "*" {
			c.transfer = true
		} else {
			return moerr.NewInvalidArgNoCtx(expr, "`storage_usage -f *` expected")
		}
	}

	expr, _ = cmd.Flags().GetString("eliminate_errors")
	if expr != "" {
		if expr == "*" {
			c.eliminateErrors = true
		} else {
			return moerr.NewInvalidArgNoCtx(expr, "`storage_usage -e *` expected")
		}
	}

	return nil
}

func (c *storageUsageHistoryArg) Run() (err error) {
	if c.detail != nil {
		return storageUsageDetails(c)
	} else if c.trace != nil {
		return storageTrace(c)
	} else if c.transfer {
		return storageUsageTransfer(c)
	} else if c.eliminateErrors {
		return storageUsageEliminateErrors(c)
	}
	return moerr.NewInvalidArgNoCtx("", c.ctx.args)
}

func (c *storageUsageHistoryArg) String() string {
	return ""
}

type manualyIgnoreArg struct {
	ctx *inspectContext
	id  uint64
}

func (c *manualyIgnoreArg) PrepareCommand() *cobra.Command {
	miCmd := &cobra.Command{
		Use:   "ckpignore",
		Short: "manually ignore table when checking checkpoint entry",
		Run:   RunFactory(c),
	}
	miCmd.Flags().Uint64P("tid", "t", 0, "format: table-id")
	return miCmd
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
	ctx     *inspectContext
	tbl     *catalog.TableEntry
	obj     *catalog.ObjectEntry
	blkn    int
	verbose common.PPLevel
}

func (c *infoArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "get dedicated debug info",
		Run:   RunFactory(c),
	}
	cmd.Flags().CountP("verbose", "v", "verbose level")
	cmd.Flags().StringP("target", "t", "*", "format: table-id")
	cmd.Flags().StringP("blk", "b", "", "format: <objectId>_<fineN>_<blkn>")
	return cmd
}

func (c *infoArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	count, _ := cmd.Flags().GetCount("verbose")
	c.verbose = switchPPL(count)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}

	baddress, _ := cmd.Flags().GetString("blk")
	c.obj, c.blkn, err = parseBlkTarget(baddress, c.tbl)
	if err != nil {
		return err
	}

	return nil
}

func (c *infoArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchemaLocked(false).Name)
	}

	if c.obj != nil {
		t = fmt.Sprintf("%s o-%s b-%d", t, c.obj.ID().String(), c.blkn)
	}

	return fmt.Sprintf("info: %v", t)
}

func (c *infoArg) Run() error {
	b := &bytes.Buffer{}
	if c.tbl != nil {
		b.WriteString(fmt.Sprintf("last_merge: %v\n", c.tbl.Stats.GetLastMerge().String()))
	}
	if c.obj != nil {
		b.WriteRune('\n')
		// b.WriteString(fmt.Sprintf("persisted_ts: %v\n", c.obj.GetObjectData().GetDeltaPersistedTS().ToString()))
		r, reason := c.obj.GetObjectData().PrepareCompactInfo()
		b.WriteString(fmt.Sprintf("prepareCompact: %v, %q\n", r, reason))
		b.WriteString(fmt.Sprintf("ppstring: %v\n", c.obj.GetObjectData().PPString(c.verbose, 0, "", c.blkn)))

		schema := c.obj.GetSchema()
		if schema.HasSortKey() {
			zm := c.obj.SortKeyZoneMap()
			var zmstr string
			if c.verbose <= common.PPL1 {
				zmstr = zm.String()
			} else if c.verbose == common.PPL2 {
				zmstr = zm.StringForCompose()
			} else {
				zmstr = zm.StringForHex()
			}
			b.WriteString(fmt.Sprintf("sort key zm: %v\n", zmstr))
		}
	}
	c.ctx.resp.Payload = b.Bytes()
	return nil
}

type mergePolicyArg struct {
	ctx               *inspectContext
	tbl               *catalog.TableEntry
	maxMergeObjN      int32
	minOsizeQualified int32
	maxOsizeObject    int32
	cnMinMergeSize    int32
	hints             []api.MergeHint

	disableDeltaLocMerge bool
}

func (c *mergePolicyArg) PrepareCommand() *cobra.Command {
	policyCmd := &cobra.Command{
		Use:   "policy",
		Short: "set merge policy for table",
		Run:   RunFactory(c),
	}
	policyCmd.Flags().StringP("target", "t", "*", "format: db.table")
	policyCmd.Flags().Int32P("maxMergeObjN", "r", common.DefaultMaxMergeObjN, "max number of objects merged for one run")
	policyCmd.Flags().Int32P("minOsizeQualified", "m", common.DefaultMinOsizeQualifiedMB, "objects whose osize are less than minOsizeQualified(MB) will be picked up to merge")
	policyCmd.Flags().Int32P("maxOsizeObject", "o", common.DefaultMaxOsizeObjMB, "merged objects' osize should be near maxOsizeObject(MB)")
	policyCmd.Flags().Int32P("minCNMergeSize", "c", common.DefaultMinCNMergeSize, "Merge task whose memory occupation exceeds minCNMergeSize(MB) will be moved to CN")
	policyCmd.Flags().Int32SliceP("mergeHints", "n", []int32{0}, "hints to merge the table")
	policyCmd.Flags().BoolP("disableDeltaLocMerge", "d", merge.DisableDeltaLocMerge.Load(), "enable merging based on delta location")
	return policyCmd
}

func (c *mergePolicyArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, _ := cmd.Flags().GetString("target")
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	c.maxMergeObjN, _ = cmd.Flags().GetInt32("maxMergeObjN")
	c.maxOsizeObject, _ = cmd.Flags().GetInt32("maxOsizeObject")
	c.minOsizeQualified, _ = cmd.Flags().GetInt32("minOsizeQualified")
	c.cnMinMergeSize, _ = cmd.Flags().GetInt32("minCNMergeSize")
	c.disableDeltaLocMerge, _ = cmd.Flags().GetBool("disableDeltaLocMerge")
	if c.maxOsizeObject > 2048 || c.minOsizeQualified > 2048 {
		return moerr.NewInvalidInputNoCtx("maxOsizeObject or minOsizeQualified should be less than 2048")
	}
	hints, _ := cmd.Flags().GetInt32Slice("mergeHints")
	for _, h := range hints {
		if _, ok := api.MergeHint_name[h]; !ok {
			return moerr.NewInvalidArgNoCtx("unspported hint %v", h)
		}
		c.hints = append(c.hints, api.MergeHint(h))
	}
	return nil
}

func (c *mergePolicyArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchemaLocked(false).Name)
	}
	return fmt.Sprintf(
		"(%s) maxMergeObjN: %v, maxOsizeObj: %vMB, minOsizeQualified: %vMB, offloadToCnSize: %vMB, hints: %v",
		t, c.maxMergeObjN, c.maxOsizeObject, c.minOsizeQualified, c.cnMinMergeSize, c.hints,
	)
}

func (c *mergePolicyArg) Run() error {
	maxosize := uint32(c.maxOsizeObject * common.Const1MBytes)
	minosize := uint32(c.minOsizeQualified * common.Const1MBytes)
	cnsize := uint64(c.cnMinMergeSize) * common.Const1MBytes

	merge.DisableDeltaLocMerge.Store(c.disableDeltaLocMerge)

	if c.tbl == nil {
		common.RuntimeMaxMergeObjN.Store(c.maxMergeObjN)
		common.RuntimeOsizeRowsQualified.Store(minosize)
		common.RuntimeMaxObjOsize.Store(maxosize)
		common.RuntimeMinCNMergeSize.Store(cnsize)
		if c.maxMergeObjN == 0 && c.minOsizeQualified == 0 {
			merge.StopMerge.Store(true)
			c.ctx.resp.Payload = []byte("auto merge is disabled")
		} else {
			merge.StopMerge.Store(false)
			c.ctx.resp.Payload = []byte("general setting has been refreshed")
		}
	} else {
		txn, err := c.ctx.db.StartTxn(nil)
		if err != nil {
			return err
		}
		err = c.ctx.db.MergeScheduler.ConfigPolicy(c.tbl, txn, &merge.BasicPolicyConfig{
			MergeMaxOneRun:    int(c.maxMergeObjN),
			ObjectMinOsize:    minosize,
			MaxOsizeMergedObj: maxosize,
			MinCNMergeSize:    cnsize,
			MergeHints:        c.hints,
		})
		if err != nil {
			return err
		}
		c.ctx.resp.Payload = []byte("success")
	}
	return nil
}

type RenameColArg struct {
	ctx              *inspectContext
	tbl              *catalog.TableEntry
	oldName, newName string
	seq              int
}

func (c *RenameColArg) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	c.tbl, _ = parseTableTarget(cmd.Flag("target").Value.String(), c.ctx.acinfo, c.ctx.db)
	c.oldName, _ = cmd.Flags().GetString("old")
	c.newName, _ = cmd.Flags().GetString("new")
	c.seq, _ = cmd.Flags().GetInt("seq")
	return nil
}

func (c *RenameColArg) PrepareCommand() *cobra.Command {
	renameColCmd := &cobra.Command{
		Use:   "rename_col",
		Short: "rename column",
		Run:   RunFactory(c),
	}
	renameColCmd.Flags().StringP("target", "t", "*", "format: db.table")
	renameColCmd.Flags().StringP("old", "o", "", "old column name")
	renameColCmd.Flags().StringP("new", "n", "", "new column name")
	renameColCmd.Flags().IntP("seq", "s", 0, "column seq")
	return renameColCmd
}

func (c *RenameColArg) String() string {
	return fmt.Sprintf("rename col: %v, %v,%v,%v", c.tbl.GetLastestSchemaLocked(false).Name, c.oldName, c.newName, c.seq)
}

func (c *RenameColArg) Run() (err error) {
	txn, _ := c.ctx.db.StartTxn(nil)
	defer func() {
		if err != nil {
			txn.Rollback(context.Background())
		}
	}()
	dbHdl, err := txn.GetDatabase(c.tbl.GetDB().GetName())
	if err != nil {
		return err
	}
	tblHdl, err := dbHdl.GetRelationByName(c.tbl.GetLastestSchemaLocked(false).Name)
	if err != nil {
		return err
	}
	err = tblHdl.AlterTable(context.Background(), api.NewRenameColumnReq(0, 0, c.oldName, c.newName, uint32(c.seq)))
	if err != nil {
		return err
	}
	return txn.Commit(context.Background())
}

type PolicyStatus struct {
	ctx      *inspectContext
	pruneId  uint64
	pruneAgo time.Duration
}

func (c *PolicyStatus) FromCommand(cmd *cobra.Command) (err error) {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	c.pruneAgo, _ = cmd.Flags().GetDuration("prune-ago")
	c.pruneId, _ = cmd.Flags().GetUint64("prune-id")
	return nil
}

func (c *PolicyStatus) PrepareCommand() *cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "policy_status",
		Short: "check cn merge status",
		Run:   RunFactory(c),
	}
	statusCmd.Flags().DurationP("prune-ago", "a", 0, "prune objects by time ago")
	statusCmd.Flags().Uint64P("prune-id", "i", 0, "prune objects by table id")
	return statusCmd
}

func (c *PolicyStatus) String() string {
	return fmt.Sprintf("policy status: prune %v ago, by id %v", c.pruneAgo, c.pruneId)
}

func (c *PolicyStatus) Run() (err error) {
	if c.pruneAgo == 0 && c.pruneId == 0 {
		c.ctx.resp.Payload = []byte(merge.ActiveCNObj.String())
		return nil
	} else {
		merge.ActiveCNObj.Prune(c.pruneId, c.pruneAgo)
		return nil
	}
}

func parseBlkTarget(address string, tbl *catalog.TableEntry) (*catalog.ObjectEntry, int, error) {
	if address == "" {
		return nil, 0, nil
	}
	parts := strings.Split(address, "_")
	if len(parts) != 3 {
		return nil, 0, moerr.NewInvalidInputNoCtx(fmt.Sprintf("invalid block address: %q", address))
	}
	uid, err := types.ParseUuid(parts[0])
	if err != nil {
		return nil, 0, err
	}
	fn, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, 0, err
	}
	bn, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, 0, err
	}
	bid := objectio.NewBlockid(&uid, uint16(fn), uint16(bn))
	objid := bid.Object()
	oentry, err := tbl.GetObjectByID(objid, false)
	if err != nil {
		return nil, 0, err
	}
	return oentry, bn, nil
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

type objectVisitor struct {
	catalog.LoopProcessor
	topk       int
	db, tbl    int
	candidates itemSet
}

func newObjectVisitor() *objectVisitor {
	v := &objectVisitor{}
	heap.Init(&v.candidates)
	return &objectVisitor{}
}

func (o *objectVisitor) OnDatabase(db *catalog.DBEntry) error {
	if !db.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}
	o.db++
	return nil
}
func (o *objectVisitor) OnTable(table *catalog.TableEntry) error {
	if !table.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}
	o.tbl++

	stat, _ := table.ObjectStats(common.PPL0, 0, -1, false)
	heap.Push(&o.candidates, mItem{objcnt: stat.ObjectCnt, did: table.GetDB().ID, tid: table.ID})
	if o.candidates.Len() > o.topk {
		heap.Pop(&o.candidates)
	}
	return nil
}

// the history of one table
// mo_ctl("dn", "inspect", "storage_usage -t accId.dbName.tableName");
//
// the history of one db
// mo_ctl("dn", "inspect", "storage_usage -t accId.dbName");
//
// the history of one acc
// mo_ctl("dn", "inspect", "storage_usage -t accId");
//
// the history of all
// mo_ctl("dn", "inspect", "storage_usage -t *");
func parseStorageUsageDetail(expr string, ac *db.AccessInfo, db *db.DB) (
	accId uint64, dbId uint64, tblId uint64, err error) {
	strs := strings.Split(expr, ".")

	if len(strs) == 0 || len(strs) > 3 {
		return 0, 0, 0, moerr.NewInvalidArgNoCtx(expr, "")
	}

	if len(strs) == 1 && strs[0] == "*" {
		return math.MaxUint32, math.MaxUint64, math.MaxUint64, nil
	}

	txn, _ := db.StartTxn(nil)
	defer txn.Commit(context.Background())

	if ac != nil {
		logutil.Infof("inspect with access info: %+v", ac)
		txn.BindAccessInfo(ac.AccountID, ac.UserID, ac.RoleID)
	}

	var id int
	if id, err = strconv.Atoi(strs[0]); err != nil {
		return 0, 0, 0, err
	}

	accId = uint64(id)
	dbId, tblId = math.MaxUint64, math.MaxUint64

	var dbHdl handle.Database
	if len(strs) >= 2 {
		dbHdl, err = txn.GetDatabase(strs[1])
		if err != nil {
			return 0, 0, 0, err
		}
		dbId = dbHdl.GetID()
	}

	if len(strs) == 3 {
		tblHdl, err := dbHdl.GetRelationByName(strs[2])
		if err != nil {
			return 0, 0, 0, err
		}

		tblId = tblHdl.ID()
	}

	return accId, dbId, tblId, nil
}

// [pos1, pos2)
func subString(src string, pos1, pos2 int) (string, error) {
	if pos2 > len(src) {
		return "", moerr.NewOutOfRangeNoCtxf("", src, pos1, " to ", pos2)
	}

	dst := make([]byte, pos2-pos1)

	copy(dst, []byte(src)[pos1:pos2])

	return string(dst), nil
}

// specify the time range
// select mo_ctl("dn", "inspect", "-t '-time 2023-12-18 14:26:14_2023-12-18 15:26:14'");
//
// specify the account id list
// select mo_ctl("dn", "inspect", "-t '-acc 0 1 2'");
//
// specify time range and account list
// select mo_ctl("dn", "inspect", "-t '-time 2023-12-18 14:26:14_2023-12-18 15:26:14 -acc 0 1 2'");
//
// no limit, show all request trace info
// select mo_ctl("dn", "inspect", "-t ");
func parseStorageUsageTrace(expr string, ac *db.AccessInfo, db *db.DB) (
	tStart, tEnd time.Time, accounts map[uint64]struct{}, err error) {

	var str string
	tIdx := strings.Index(expr, "-time")
	if tIdx != -1 {
		dash := strings.Index(expr, "_")
		if dash == -1 {
			err = moerr.NewInvalidArgNoCtx(expr, "")
			return
		}
		str, err = subString(expr, tIdx+len("-time")+len(" "), dash)
		if err != nil {
			return
		}

		tStart, err = time.Parse("2006-01-02 15:04:05", str)
		if err != nil {
			return
		}

		str, err = subString(expr, dash+len("_"), dash+len("_")+len("2006-01-02 15:04:05"))
		if err != nil {
			return
		}
		tEnd, err = time.Parse("2006-01-02 15:04:05", str)
		if err != nil {
			return
		}
	}

	aIdx := strings.Index(expr, "-acc")
	if aIdx != -1 {
		stop := len(expr)
		if aIdx < tIdx {
			stop = tIdx
		}
		str, err = subString(expr, aIdx+len("-acc")+len(" "), stop)
		if err != nil {
			return
		}
		accs := strings.Split(str, " ")

		accounts = make(map[uint64]struct{})

		var id int
		for i := range accs {
			id, err = strconv.Atoi(accs[i])
			if err != nil {
				return
			}
			accounts[uint64(id)] = struct{}{}
		}
	}

	return
}

func checkUsageData(data logtail.UsageData, c *storageUsageHistoryArg) bool {
	if c.detail.accId == math.MaxUint32 {
		return true
	}

	if c.detail.accId != data.AccId {
		return false
	}

	if c.detail.dbI == math.MaxUint64 {
		return true
	}

	if c.detail.dbI != data.DbId {
		return false
	}

	if c.detail.tblId == math.MaxUint64 {
		return true
	}

	return c.detail.tblId == data.TblId
}

func storageUsageDetails(c *storageUsageHistoryArg) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	entries := c.ctx.db.BGCheckpointRunner.GetAllCheckpoints()

	versions := make([]uint32, 0)
	locations := make([]objectio.Location, 0)

	for idx := range entries {
		// if entries[idx].GetVersion() < logtail.CheckpointVersion11 {
		// 	continue
		// }
		versions = append(versions, entries[idx].GetVersion())
		locations = append(locations, entries[idx].GetLocation())
	}

	// remove the old version
	entries = entries[len(entries)-len(versions):]

	var usageInsData [][]logtail.UsageData
	var usageDelData [][]logtail.UsageData

	if usageInsData, usageDelData, err = logtail.GetStorageUsageHistory(
		ctx, c.ctx.db.Runtime.SID(), locations, versions,
		c.ctx.db.Runtime.Fs.Service, common.DebugAllocator); err != nil {
		return err
	}

	txn, _ := c.ctx.db.StartTxn(nil)
	defer txn.Commit(context.Background())

	getDbAndTblNames := func(dbId, tblId uint64) (string, string) {
		h, _ := txn.GetDatabaseByID(dbId)
		if h == nil {
			return "deleted", "deleted"
		}

		r, _ := h.GetRelationByID(tblId)
		if r == nil {
			return h.GetName(), "deleted"
		}
		return h.GetName(), r.Schema(false).(*catalog.Schema).Name
	}

	getAllDbAndTblNames := func(usages []logtail.UsageData) (dbs, tbls []string, maxDbLen, maxTblLen int) {
		for idx := range usages {
			if checkUsageData(usages[idx], c) {
				dbName, tblName := getDbAndTblNames(usages[idx].DbId, usages[idx].TblId)
				dbs = append(dbs, dbName)
				tbls = append(tbls, tblName)

				maxDbLen = int(math.Max(float64(maxDbLen), float64(len(dbName))))
				maxTblLen = int(math.Max(float64(maxTblLen), float64(len(tblName))))
			}
		}
		return
	}

	formatOutput := func(
		dst *bytes.Buffer, data logtail.UsageData,
		dbName, tblName string, maxDbLen, maxTblLen int, hint string) float64 {

		size := float64(data.Size) / 1048576

		dst.WriteString(fmt.Sprintf("\t[(acc)-%-10d (%*s)-%-10d (%*s)-%-10d] %s -> %15.6f (mb)\n",
			data.AccId, maxDbLen, dbName, data.DbId,
			maxTblLen, tblName, data.TblId, hint, size))

		return size
	}

	b := &bytes.Buffer{}
	ckpType := []string{"G", "I"}

	totalSize := 0.0
	for x := range entries {
		eachCkpTotal := 0.0

		b.WriteString(fmt.Sprintf("CKP[%s]: %s\n", ckpType[entries[x].GetType()],
			time.Unix(0, entries[x].GetEnd().Physical())))

		dbNames, tblNames, dbLen, tblLen := getAllDbAndTblNames(usageInsData[x])
		for _, data := range usageInsData[x] {
			if checkUsageData(data, c) {
				eachCkpTotal += formatOutput(b, data, dbNames[0], tblNames[0], dbLen, tblLen, "insert")
				dbNames = dbNames[1:]
				tblNames = tblNames[1:]
			}
		}

		dbNames, tblNames, dbLen, tblLen = getAllDbAndTblNames(usageDelData[x])
		for _, data := range usageDelData[x] {
			if checkUsageData(data, c) {
				eachCkpTotal -= formatOutput(b, data, dbNames[0], tblNames[0], dbLen, tblLen, "delete")
				dbNames = dbNames[1:]
				tblNames = tblNames[1:]
			}
		}

		if eachCkpTotal != 0 {
			b.WriteString(fmt.Sprintf("\n\taccumulation: %f (mb)\n", eachCkpTotal))
		}

		totalSize += eachCkpTotal

		b.WriteByte('\n')
	}

	b.WriteString(fmt.Sprintf(
		"total accumulation in all ckps: %f (mb), current tn cache mem used: %f (mb)\n",
		totalSize, c.ctx.db.GetUsageMemo().MemoryUsed()))

	c.ctx.resp.Payload = b.Bytes()
	return nil
}

func storageTrace(c *storageUsageHistoryArg) (err error) {

	filter := func(accId uint64, stamp time.Time) bool {
		if !c.trace.tStart.IsZero() {
			if stamp.UTC().Add(time.Hour*8).Before(c.trace.tStart) ||
				stamp.UTC().Add(time.Hour*8).After(c.trace.tEnd) {
				return false
			}
		}

		if len(c.trace.accounts) != 0 {
			if _, ok := c.trace.accounts[accId]; !ok {
				return false
			}
		}
		return true
	}

	var b bytes.Buffer

	memo := c.ctx.db.GetUsageMemo()
	accIds, stamps, sizes, hints := memo.GetAllReqTrace()

	preIdx := -1
	for idx := range stamps {
		if !filter(accIds[idx], stamps[idx]) {
			continue
		}
		if preIdx == -1 || !stamps[preIdx].Equal(stamps[idx]) {
			preIdx = idx
			b.WriteString(fmt.Sprintf("\n%s:\n", stamps[idx].String()))
		}

		size := float64(sizes[idx]) / 1048576
		b.WriteString(fmt.Sprintf("\taccount id: %-10d\tsize: %15.6f\thint: %s\n",
			accIds[idx], size, hints[idx]))
	}

	b.WriteString("\n")

	c.ctx.resp.Payload = b.Bytes()

	return nil
}

func storageUsageTransfer(c *storageUsageHistoryArg) (err error) {
	cnt, size, err := logtail.CorrectUsageWrongPlacement(c.ctx.db.Catalog)
	if err != nil {
		return err
	}

	c.ctx.out.Write([]byte(fmt.Sprintf("transferred %d tbl, %f mb; ", cnt, size)))
	return
}

func storageUsageEliminateErrors(c *storageUsageHistoryArg) (err error) {
	entries := c.ctx.db.BGCheckpointRunner.GetAllCheckpoints()
	if len(entries) == 0 {
		return moerr.NewNotSupportedNoCtx("please execute this cmd after at least one checkpoint has been generated")
	}
	end := entries[len(entries)-1].GetEnd()
	cnt := logtail.EliminateErrorsOnCache(c.ctx.db.Catalog, end)
	c.ctx.out.Write([]byte(fmt.Sprintf("%d tables backed to the track. ", cnt)))

	return nil
}

type transferArg struct {
	mem  int
	disk int
	show bool
}

func (c *transferArg) PrepareCommand() *cobra.Command {
	transferCmd := &cobra.Command{
		Use:   "transfer",
		Short: "set transfer ttl",
		Run:   RunFactory(c),
	}
	transferCmd.Flags().IntP("mem", "m", 5, "set transfer page memory ttl (s)")
	transferCmd.Flags().IntP("disk", "d", 3, "set transfer page disk ttl (min)")
	transferCmd.Flags().BoolP("show", "s", false, "show transfer ttl")

	return transferCmd
}

func (c *transferArg) FromCommand(cmd *cobra.Command) (err error) {
	c.mem, _ = cmd.Flags().GetInt("mem")
	c.disk, _ = cmd.Flags().GetInt("disk")
	c.show, _ = cmd.Flags().GetBool("show")
	return nil
}

func (c *transferArg) String() string {
	return fmt.Sprintf("transfer page ttl, mem:%v, disk:%v", model.GetTTL(), model.GetDiskTTL())
}

func (c *transferArg) Run() error {
	if c.show {
		c.show = false
		return nil
	}
	model.SetTTL(time.Duration(c.mem) * time.Second)
	model.SetDiskTTL(time.Duration(c.disk) * time.Minute)
	return nil
}
