package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/spf13/cobra"
)

// region: merge root

type mergeArg struct{}

func (c *mergeArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "merge",
		Short: "manage merging things",
		Run:   RunFactory(c),
	}
	cmd.PersistentFlags().StringP("target", "t", "*", "format: db.table")

	wasmArg := &mergeWasmArg{}
	cmd.AddCommand(wasmArg.PrepareCommand())

	mergeSwitchArg := &mergeSwitchArg{}
	cmd.AddCommand(mergeSwitchArg.PrepareCommand())

	mergeShowArg := &mergeShowArg{}
	cmd.AddCommand(mergeShowArg.PrepareCommand())
	return cmd
}

func (c *mergeArg) FromCommand(cmd *cobra.Command) error { return nil }
func (c *mergeArg) String() string                       { return "merge" }
func (c *mergeArg) Run() error                           { return nil }

// endregion: merge root

// region: merge switch

type mergeSwitchArg struct {
	ctx    *inspectContext
	tbl    *catalog.TableEntry
	enable bool
}

func (arg *mergeSwitchArg) Run() error {
	if arg.tbl == nil {
		if arg.enable {
			arg.ctx.db.MergeScheduler.ResumeAll()
			arg.ctx.resp.Payload = []byte("auto merge is enabled")
		} else {
			arg.ctx.db.MergeScheduler.PauseAll()
			arg.ctx.resp.Payload = []byte("auto merge is disabled")
		}
		return nil
	}
	if arg.enable {
		arg.ctx.db.MergeScheduler.ResumeTable(arg.tbl)
		arg.ctx.resp.Payload = []byte(fmt.Sprintf("merge enabled for table %d-%s",
			arg.tbl.ID, arg.tbl.GetLastestSchema(false).Name))
	} else {
		arg.ctx.db.MergeScheduler.PauseTable(arg.tbl)
		arg.ctx.resp.Payload = []byte(fmt.Sprintf("merge disabled for table %d-%s",
			arg.tbl.ID, arg.tbl.GetLastestSchema(false).Name))
	}
	return nil
}

func (arg *mergeSwitchArg) String() string {
	t := "*"
	if arg.tbl != nil {
		t = fmt.Sprintf("%d-%s", arg.tbl.ID, arg.tbl.GetLastestSchema(false).Name)
	}
	action := "enabled"
	if !arg.enable {
		action = "disabled"
	}
	return fmt.Sprintf("merge is %s for table (%s)", action, t)
}

func (arg *mergeSwitchArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "switch [on|off]",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return moerr.NewInvalidInputNoCtxf("accepts 1 arg(s), received %d", len(args))
			}
			if args[0] != "on" && args[0] != "off" {
				return moerr.NewInvalidInputNoCtxf("invalid action %s, should be on or off", args[0])
			}
			return nil
		},
		Short: "switch merge for all or a table",
		Run:   RunFactory(arg),
	}
	return cmd
}

func (arg *mergeSwitchArg) FromCommand(cmd *cobra.Command) (err error) {
	arg.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	action := cmd.Flags().Args()[0]
	arg.enable = action == "on"

	address, err := cmd.Flags().GetString("target")
	if err != nil {
		return err
	}
	arg.tbl, err = parseTableTarget(address, arg.ctx.acinfo, arg.ctx.db)
	return err
}

// endregion: merge switch

// region: merge show

type mergeShowArg struct {
	ctx          *inspectContext
	tbl          *catalog.TableEntry
	vacuumDetail bool
}

func (arg *mergeShowArg) Run() error {
	out := bytes.Buffer{}
	answer := arg.ctx.db.MergeScheduler.Query(arg.tbl)
	out.WriteString(fmt.Sprintf("auto merge for all: %v, msg queue len: %d", answer.GlobalAutoMergeOn, answer.MsgQueueLen))
	if arg.tbl != nil {
		id, name := arg.tbl.GetID(), arg.tbl.GetLastestSchema(false).Name
		out.WriteString(fmt.Sprintf("\ntable info: %d-%s", id, name))
		out.WriteString(fmt.Sprintf("\n\tauto merge: %v", answer.AutoMergeOn))
		out.WriteString(fmt.Sprintf("\n\ttotal tasks: %d", answer.MergedCnt))
		out.WriteString(fmt.Sprintf("\n\tnext check due: %v", answer.NextCheckDue))
		out.WriteString(fmt.Sprintf("\n\tmerge tasks in queue: %d", answer.PendingMergeCnt))

		// check object distribution
		// collect all object stats
		it := arg.tbl.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
		statsList := make([]*objectio.ObjectStats, 0, 64)
		for it.Next() {
			obj := it.Item()
			if !merge.ObjectValid(obj) {
				continue
			}
			statsList = append(statsList, obj.GetObjectStats())
		}

		OutputLayerZeroStats(&out, arg.tbl, statsList, merge.DefaultLayerZeroOpts)
		OutputOverlapStats(&out, statsList, merge.DefaultOverlapOpts.Clone().WithFurtherStat(true))
		OutputVacuumStats(&out, arg.tbl, merge.NewVacuumOpts().WithEnableDetail(arg.vacuumDetail))
		// calc tombstone stats
	}
	arg.ctx.resp.Payload = out.Bytes()
	return nil
}

func OutputLayerZeroStats(out *bytes.Buffer, tbl *catalog.TableEntry, statsList []*objectio.ObjectStats, opts *merge.LayerZeroOpts) {
	layerZeroStats := merge.CalculateLayerZeroStats(context.Background(), statsList, time.Since(tbl.Stats.GetLastMergeTime()), opts)
	out.WriteString(fmt.Sprintf("\nlevel 0 basic stats  : %s", layerZeroStats.String()))
}

func OutputOverlapStats(out *bytes.Buffer, statsList []*objectio.ObjectStats, opts *merge.OverlapOpts) {
	lvCnt := 8
	leveledStats := make([][]*objectio.ObjectStats, lvCnt)
	for _, stat := range statsList {
		leveledStats[stat.GetLevel()] = append(leveledStats[stat.GetLevel()], stat)
	}
	for i := range lvCnt {
		if len(leveledStats[i]) < 2 {
			continue
		}
		overlapStats, _ := merge.CalculateOverlapStats(context.Background(), leveledStats[i], opts)
		out.WriteString(fmt.Sprintf("\nlevel %d overlap stats: %s", i, overlapStats.String()))
	}
}

func OutputVacuumStats(out *bytes.Buffer, tbl *catalog.TableEntry, opts *merge.VacuumOpts) {
	stats, err := merge.CalculateVacuumStats(context.Background(), tbl, opts)
	if err != nil {
		out.WriteString(fmt.Sprintf("\nvacuum stats: %s", err))
		return
	}
	out.WriteString(fmt.Sprintf("\nvacuum stats: %s", stats.String()))
}

func (arg *mergeShowArg) String() string {
	t := "*"
	if arg.tbl != nil {
		t = fmt.Sprintf("%d-%s", arg.tbl.ID, arg.tbl.GetLastestSchema(false).Name)
	}
	return fmt.Sprintf("merge status for table %s", t)
}

func (arg *mergeShowArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show merge status",
		Run:   RunFactory(arg),
	}
	cmd.Flags().BoolVar(&arg.vacuumDetail, "vacuum-detail", false, "show vacuum detail(IO involved)")
	return cmd
}

func (arg *mergeShowArg) FromCommand(cmd *cobra.Command) (err error) {
	arg.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, err := cmd.Flags().GetString("target")
	if err != nil {
		return err
	}
	arg.tbl, err = parseTableTarget(address, arg.ctx.acinfo, arg.ctx.db)
	return err
}

// endregion: merge show

// region: merge wasm

func preparePlugin(address string) (*extism.Plugin, error) {
	extism.SetLogLevel(extism.LogLevelInfo)
	wurl, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	var manifest extism.Manifest
	if wurl.Scheme == "https" || wurl.Scheme == "http" {
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmUrl{
					Url: address,
				},
			},
		}
	} else {
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmFile{
					Path: address,
				},
			},
		}
	}
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	ctx := context.Background()
	return extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
}

type mergeWasmArg struct {
	ctx      *inspectContext
	wasm     string
	function string
	dryrun   bool
	tbl      *catalog.TableEntry
}

func (c *mergeWasmArg) FromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	if c.wasm == "" {
		return moerr.NewInvalidInputNoCtx("wasm is required")
	}
	address, _ := cmd.Flags().GetString("target")
	var err error
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("target table is required")
	}
	return nil
}

func (c *mergeWasmArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wasm",
		Short: "run wasm policy for table",
		Run:   RunFactory(c),
	}
	cmd.Flags().StringVarP(&c.wasm, "wasm", "w", "", "wasm file path")
	cmd.Flags().StringVarP(&c.function, "function", "f", "filterStats", "wasm function")
	cmd.Flags().BoolVarP(&c.dryrun, "dryrun", "d", false, "dryrun")
	return cmd
}

func (c *mergeWasmArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchemaLocked(false).Name)
	}
	return fmt.Sprintf("wasm: %v, dryrun: %v, table: %v", c.wasm, c.dryrun, t)
}

func (c *mergeWasmArg) Run() error {
	plugin, err := preparePlugin(c.wasm)
	if err != nil {
		return err
	}

	input := collectObjectStats(c)
	_, out, err := plugin.Call(c.function, input)
	if err != nil {
		return err
	}

	selected := objectio.ObjectStatsSlice(out)
	if c.dryrun || len(selected) < 2 {
		c.ctx.resp.Payload = []byte(fmt.Sprintf("dryrun(%d):\n", len(selected)/objectio.ObjectStatsLen))
		for i := 0; i < selected.Len(); i++ {
			stat := selected.Get(i)
			c.ctx.resp.Payload = append(c.ctx.resp.Payload, []byte(fmt.Sprintf("%s %d %d\n", stat.ObjectName().ObjectId().ShortStringEx(), stat.OriginSize(), stat.Rows()))...)
		}
		return nil
	}
	return c.dispatchMergeTask(c.tbl, selected)
}

func collectObjectStats(c *mergeWasmArg) []byte {
	it := c.tbl.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	input := make([]byte, 0, objectio.ObjectStatsLen*128)
	for it.Next() {
		obj := it.Item()
		if !merge.ObjectValid(obj) {
			continue
		}
		input = append(input, obj.ObjectStats[:]...)
	}
	return input
}

func (c *mergeWasmArg) dispatchMergeTask(tbl *catalog.TableEntry, stats objectio.ObjectStatsSlice) error {
	mObjs := make([]*catalog.ObjectEntry, 0, stats.Len())
	for i := 0; i < stats.Len(); i++ {
		entry, err := tbl.GetObjectByID(stats.Get(i).ObjectName().ObjectId(), false)
		if err != nil {
			continue
		}
		mObjs = append(mObjs, entry)
	}

	scopes := make([]common.ID, 0, len(mObjs))
	for _, obj := range mObjs {
		scopes = append(scopes, *obj.AsCommonID())
	}
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return jobs.NewMergeObjectsTask(ctx, txn, mObjs, c.ctx.db.Runtime, common.DefaultMaxOsizeObjBytes, false)
	}
	task, err := c.ctx.db.Runtime.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if !errors.Is(err, tasks.ErrScheduleScopeConflict) {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("schedule-merge-task"),
				common.AnyField("error", err),
				common.AnyField("task", task.Name()),
			)
		}
		return err
	}
	err = task.WaitDone(context.Background())
	if err != nil {
		return err
	}
	tbl.Stats.SetLastMergeTime()
	return nil
}

// endregion: merge wasm
