// Copyright 2024 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
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

	mergeSwitchArg := &mergeSwitchArg{}
	cmd.AddCommand(mergeSwitchArg.PrepareCommand())

	mergeShowArg := &mergeShowArg{}
	cmd.AddCommand(mergeShowArg.PrepareCommand())

	mergeTriggerArg := &mergeTriggerArg{}
	cmd.AddCommand(mergeTriggerArg.PrepareCommand())
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
		arg.ctx.db.MergeScheduler.ResumeTable(catalog.ToMergeTable(arg.tbl))
		arg.ctx.resp.Payload = []byte(fmt.Sprintf("merge enabled for table %d-%s",
			arg.tbl.ID, arg.tbl.GetLastestSchema(false).Name))
	} else {
		arg.ctx.db.MergeScheduler.PauseTable(catalog.ToMergeTable(arg.tbl))
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
				return moerr.NewInvalidInputNoCtxf(
					"accepts 1 arg(s), received %d", len(args),
				)
			}
			if args[0] != "on" && args[0] != "off" {
				return moerr.NewInvalidInputNoCtxf(
					"invalid action %s, should be on or off", args[0],
				)
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
	ctx *inspectContext
	tbl *catalog.TableEntry

	handleBigOld bool

	lnFitPolyDegree int

	vacuumDetail       bool
	vacuumCheckBigOnly bool
}

func (arg *mergeShowArg) Run() error {
	out := bytes.Buffer{}
	var target catalog.MergeTable
	if arg.tbl != nil {
		target = catalog.ToMergeTable(arg.tbl)
	}
	answer := arg.ctx.db.MergeScheduler.Query(target)
	out.WriteString(fmt.Sprintf(
		"auto merge for all: %v, msg queue len: %d\n",
		answer.GlobalAutoMergeOn,
		answer.MsgQueueLen,
	))
	if arg.tbl != nil {
		id, name := arg.tbl.GetID(), arg.tbl.GetLastestSchema(false).Name
		out.WriteString(fmt.Sprintf("\ntable info: %d-%s", id, name))
		out.WriteString(fmt.Sprintf("\n\tauto merge: %v", answer.AutoMergeOn))
		out.WriteString(fmt.Sprintf("\n\ttotal data merge tasks: %d", answer.DataMergeCnt))
		out.WriteString(fmt.Sprintf(
			"\n\ttotal tombstone merge tasks: %d",
			answer.TombstoneMergeCnt,
		))
		out.WriteString(fmt.Sprintf(
			"\n\tlast merge time: %s ago",
			time.Since(arg.tbl.Stats.GetLastMergeTime()).Round(time.Minute),
		))
		out.WriteString(fmt.Sprintf(
			"\n\tnext check due: %v",
			answer.NextCheckDue.Round(time.Second),
		))
		out.WriteString(fmt.Sprintf(
			"\n\tmerge tasks in queue: %d", answer.PendingMergeCnt,
		))
		out.WriteString(fmt.Sprintf(
			"\n\tvaccum trig count: %v", answer.VaccumTrigCount,
		))
		out.WriteString(fmt.Sprintf(
			"\n\tlast vaccum check: %s ago", answer.LastVaccumCheck.Round(time.Second),
		))
		if len(answer.Triggers) > 0 {
			out.WriteString(fmt.Sprintf("\n\ttriggers: %s", answer.Triggers))
		}
		if len(answer.BaseTrigger) > 0 {
			out.WriteString(fmt.Sprintf("\n\tbase trigger: %s", answer.BaseTrigger))
		}
		// check object distribution
		// collect all object stats
		mergeTable := catalog.ToMergeTable(arg.tbl)
		statsList := make([]*objectio.ObjectStats, 0, 64)
		checkTime := mergeTable.IsSpecialBigTable() && !arg.handleBigOld
		for obj := range mergeTable.IterDataItem() {
			stat := obj.GetObjectStats()
			if checkTime &&
				obj.GetCreatedAt().Physical() < merge.ReleaseDate &&
				stat.OriginSize() > common.DefaultMinOsizeQualifiedBytes {
				continue
			}
			statsList = append(statsList, stat)
		}

		out.WriteString("\n")
		OutputLayerZeroStats(&out, arg.tbl, statsList, merge.DefaultLayerZeroOpts)
		out.WriteString("\n")
		OutputOverlapStats(
			&out,
			statsList,
			merge.NewOverlapOptions().
				WithFurtherStat(true).
				WithFitPolynomialDegree(arg.lnFitPolyDegree),
		)
		out.WriteString("\n")
		OutputVacuumStats(
			&out,
			mergeTable,
			merge.NewVacuumOpts().
				WithEnableDetail(arg.vacuumDetail).
				WithCheckBigOnly(arg.vacuumCheckBigOnly),
		)
	}
	arg.ctx.resp.Payload = out.Bytes()
	return nil
}

func OutputLayerZeroStats(
	out *bytes.Buffer,
	tbl *catalog.TableEntry,
	statsList []*objectio.ObjectStats,
	opts *merge.LayerZeroOpts,
) {
	layerZeroStats := merge.CalculateLayerZeroStats(
		context.Background(), statsList, time.Since(tbl.Stats.GetLastMergeTime()), opts,
	)
	out.WriteString(fmt.Sprintf("\nlevel 0 basic stats  : %s", layerZeroStats.String()))
}

func OutputOverlapStats(
	out *bytes.Buffer,
	statsList []*objectio.ObjectStats,
	opts *merge.OverlapOpts,
) {
	lvCnt := 8
	leveledStats := make([][]*objectio.ObjectStats, lvCnt)
	for _, stat := range statsList {
		leveledStats[stat.GetLevel()] = append(leveledStats[stat.GetLevel()], stat)
	}
	for i := range lvCnt {
		if len(leveledStats[i]) < 2 {
			continue
		}
		overlapStats, err := merge.CalculateOverlapStats(
			context.Background(), leveledStats[i], opts,
		)
		if err != nil {
			out.WriteString(fmt.Sprintf(
				"\nlevel %d overlap stats: %s", i, err,
			))
			continue
		} else {
			out.WriteString(fmt.Sprintf(
				"\nlevel %d overlap stats: %s", i, overlapStats.String(),
			))
		}
	}
}

func OutputVacuumStats(
	out *bytes.Buffer,
	tbl catalog.MergeTable,
	opts *merge.VacuumOpts,
) {
	stats, err := merge.CalculateVacuumStats(context.Background(), tbl, opts, time.Now())
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
	return fmt.Sprintf("merge show status for table %s", t)
}

func (arg *mergeShowArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show merge status",
		Run:   RunFactory(arg),
	}
	cmd.Flags().BoolVar(&arg.handleBigOld, "handle-big-old", false, "handle big old data objects for special big table")
	cmd.Flags().MarkHidden("handle-big-old")
	cmd.Flags().BoolVar(&arg.vacuumDetail, "vacuum-detail", false, "show vacuum detail(IO involved)")
	cmd.Flags().BoolVar(&arg.vacuumCheckBigOnly, "vacuum-big-only", false, "check big only")
	cmd.Flags().IntVar(&arg.lnFitPolyDegree, "layer-poly-degree", 0, "fit polynomial degree for layers")
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

// region: trigger

type mergeTriggerArg struct {
	ctx *inspectContext
	tbl *catalog.TableEntry

	kind        string
	patchExpire time.Duration

	handleBigOld bool

	vacuumAll      bool
	vacuumTopK     int
	vacuumStart    int
	vacuumEnd      int
	vacuumDuration time.Duration

	lnStartlv               int
	lnEndlv                 int
	minPointDepthPerCluster int

	l0OneShot  bool
	l0Start    int
	l0End      int
	l0Duration time.Duration
	l0CPoints  []float64

	tombstoneOneShot bool
	tombstoneL1Size  int
	tombstoneL1Count int
	tombstoneL2Count int
}

func (arg *mergeTriggerArg) String() string {
	switch arg.kind {
	case "none":
		return fmt.Sprintf("trigger nothing for table %s", arg.tbl.GetNameDesc())
	case "l0":
		return fmt.Sprintf("trigger l0 merge for table %s, oneshot: %v", arg.tbl.GetNameDesc(), arg.l0OneShot)
	case "ln":
		return fmt.Sprintf("trigger ln merge for table %s, startlv: %d, endlv: %d, minPointDepthPerCluster: %d",
			arg.tbl.GetNameDesc(), arg.lnStartlv, arg.lnEndlv, arg.minPointDepthPerCluster)
	case "tombstone":
		return fmt.Sprintf("trigger tombstone merge for table %s, oneshot: %v", arg.tbl.GetNameDesc(), arg.tombstoneOneShot)
	case "vacuum":
		return fmt.Sprintf("trigger vacuum for table %s, start: %d, end: %d, duration: %s, all: %v",
			arg.tbl.GetNameDesc(), arg.vacuumStart, arg.vacuumEnd, arg.vacuumDuration, arg.vacuumAll)
	}
	return ""
}

func (arg *mergeTriggerArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "trigger merge",
		Run:   RunFactory(arg),
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().StringVar(&arg.kind, "kind", "none", "trigger kind(none, l0, ln, tombstone, vacuum)")
	cmd.Flags().DurationVar(&arg.patchExpire, "patch-expire", 0, "patch expire, keep it zero will trigger once")

	cmd.Flags().BoolVar(&arg.handleBigOld, "handle-big-old", false, "handle big old data objects for special big table")
	cmd.Flags().MarkHidden("handle-big-old")

	cmd.Flags().BoolVar(&arg.vacuumAll, "vacuum-all", false, "vacuum all tombstones, including those < 128MB")
	cmd.Flags().IntVar(&arg.vacuumTopK, "vacuum-topk", merge.DefaultVacuumOpts.HollowTopK, "vacuum top k")
	cmd.Flags().IntVar(&arg.vacuumStart, "vacuum-start", merge.DefaultVacuumOpts.StartScore, "vacuum start score")
	cmd.Flags().IntVar(&arg.vacuumEnd, "vacuum-end", merge.DefaultVacuumOpts.EndScore, "vacuum end score")
	cmd.Flags().DurationVar(&arg.vacuumDuration, "vacuum-duration", merge.DefaultVacuumOpts.Duration, "vacuum recession duration")

	cmd.Flags().IntVar(&arg.lnStartlv, "ln-start", 1, "layer start")
	cmd.Flags().IntVar(&arg.lnEndlv, "ln-end", 7, "layer end")
	cmd.Flags().IntVar(&arg.minPointDepthPerCluster, "ln-min-point-depth", merge.DefaultOverlapOpts.MinPointDepthPerCluster, "min point depth per cluster")

	cmd.Flags().BoolVar(&arg.l0OneShot, "l0-oneshot", false, "merge all l0 objects")
	cmd.Flags().Float64SliceVar(&arg.l0CPoints, "l0-cpoints", merge.DefaultLayerZeroOpts.CPoints[:], "l0 recession control points, cubic bezier curve")
	cmd.Flags().DurationVar(&arg.l0Duration, "l0-duration", merge.DefaultLayerZeroOpts.Duration, "l0 recession duration")
	cmd.Flags().IntVar(&arg.l0Start, "l0-start", merge.DefaultLayerZeroOpts.Start, "l0 start count")
	cmd.Flags().IntVar(&arg.l0End, "l0-end", merge.DefaultLayerZeroOpts.End, "l0 end count")

	cmd.Flags().BoolVar(&arg.tombstoneOneShot, "tombstone-oneshot", false, "merge all tombstone objects")
	cmd.Flags().IntVar(&arg.tombstoneL1Size, "tombstone-l1-size", merge.DefaultTombstoneOpts.L1Size, "tombstone l1 size")
	cmd.Flags().IntVar(&arg.tombstoneL1Count, "tombstone-l1-count", merge.DefaultTombstoneOpts.L1Count, "tombstone l1 count")
	cmd.Flags().IntVar(&arg.tombstoneL2Count, "tombstone-l2-count", merge.DefaultTombstoneOpts.L2Count, "tombstone l2 count")

	return cmd
}

func (arg *mergeTriggerArg) FromCommand(cmd *cobra.Command) (err error) {
	arg.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	address, err := cmd.Flags().GetString("target")
	if err != nil {
		return err
	}
	arg.tbl, err = parseTableTarget(address, arg.ctx.acinfo, arg.ctx.db)

	if arg.patchExpire > 0 {
		if arg.l0OneShot {
			return moerr.NewInvalidInputNoCtxf("l0 oneshot cannot be used with patch")
		}
		if arg.tombstoneOneShot {
			return moerr.NewInvalidInputNoCtxf("tombstone oneshot cannot be used with patch")
		}
	}

	return err
}

func (arg *mergeTriggerArg) Run() error {
	mergeTable := catalog.ToMergeTable(arg.tbl)
	trigger := merge.NewMMsgTaskTrigger(mergeTable).
		WithByUser(true).
		WithHandleBigOld(arg.handleBigOld)
	if arg.patchExpire > 0 {
		// mark the patch trigger not by user, so that it will be ignored if auto merge is off
		trigger.WithExpire(time.Now().Add(arg.patchExpire)).WithByUser(false)
	}

	switch arg.kind {
	case "none":
		return arg.ctx.db.MergeScheduler.SendTrigger(trigger)
	case "l0":
		opts := merge.NewLayerZeroOpts()
		if arg.l0OneShot {
			opts.WithToleranceDegressionCurve(1, 1, 0, [4]float64{0, 0, 1, 1})
		} else {
			opts.WithToleranceDegressionCurve(
				arg.l0Start,
				arg.l0End,
				arg.l0Duration,
				[4]float64{
					arg.l0CPoints[0],
					arg.l0CPoints[1],
					arg.l0CPoints[2],
					arg.l0CPoints[3],
				},
			)
		}
		trigger.WithL0(opts)
		return arg.ctx.db.MergeScheduler.SendTrigger(trigger)
	case "ln":
		opts := merge.NewOverlapOptions().
			WithMinPointDepthPerCluster(arg.minPointDepthPerCluster)
		trigger.WithLn(arg.lnStartlv, arg.lnEndlv, opts)
		return arg.ctx.db.MergeScheduler.SendTrigger(trigger)
	case "tombstone":
		opts := merge.NewTombstoneOpts().
			WithOneShot(arg.tombstoneOneShot).
			WithL1(arg.tombstoneL1Size, arg.tombstoneL1Count).
			WithL2Count(arg.tombstoneL2Count)
		trigger.WithTombstone(opts)
		return arg.ctx.db.MergeScheduler.SendTrigger(trigger)
	case "vacuum":
		opts := merge.NewVacuumOpts().
			WithStartScore(arg.vacuumStart).
			WithEndScore(arg.vacuumEnd).
			WithDuration(arg.vacuumDuration).
			WithCheckBigOnly(!arg.vacuumAll)
		trigger.WithVacuumCheck(opts)
		return arg.ctx.db.MergeScheduler.SendTrigger(trigger)
	default:
		return moerr.NewInvalidInputNoCtxf("invalid kind: %s", arg.kind)
	}
}

// endregion: trigger
