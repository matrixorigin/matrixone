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

package timewin

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(TimeWin)

const (
	receive     = 0
	fill        = 1
	end         = 2
	flush       = 3
	nextWindow  = 4
	nextBatch   = 5
	firstWindow = 6
	interval    = 7
	// resumeAfterFlush advances to the next active window after an internal
	// result flush. The boundary window was already published by the previous
	// aggregate generation, so this transition must not emit it again.
	resumeAfterFlush = 8
	// boundedEmpty emits a query-bounded grid when the child has no rows and
	// therefore there is no observed timestamp to seed firstWindow.
	boundedEmpty = 9
)

type container struct {
	bat    *batch.Batch
	colCnt int
	i      int

	aggExe []colexec.ExprEvalVector
	aggVec [][][]*vector.Vector

	partExe []colexec.ExpressionExecutor
	partVec [][]*vector.Vector
	// partSet broadcasts one input row's partition key across a whole flushed
	// batch: every window in a flush belongs to a single partition, because
	// the operator flushes at each boundary.
	partSet []func(v, w *vector.Vector, sel int64, length int) error
	partOut []*vector.Vector

	tsExe colexec.ExpressionExecutor
	tsVec []*vector.Vector
	tsOid types.T

	startExe colexec.ExpressionExecutor
	startVec *vector.Vector
	endExe   colexec.ExpressionExecutor
	endVec   *vector.Vector
	// Bounds are constant expressions inferred from the query predicates and
	// evaluated once per Prepare, so prepared parameters remain supported.
	gapFillStartExe colexec.ExpressionExecutor
	gapFillEndExe   colexec.ExpressionExecutor
	gapFillStart    types.Datetime
	gapFillEnd      types.Datetime
	gapFillRows     int64
	// boundedGapFill is the per-execution decision to use the inferred bounds.
	// Zero temporal sentinels cannot participate in the regular DATETIME grid,
	// so those executions fall back to the legacy observed-range GAPFILL path.
	boundedGapFill  bool
	syntheticBounds bool

	status int32
	end    bool

	group int
	aggs  []aggexec.AggFuncExec

	prepareParamKind aggexec.PrepareParamKindStates

	wStart []types.Datetime
	wEnd   []types.Datetime

	curVecIdx int
	curRowIdx int

	left  types.Datetime
	right types.Datetime

	preVecIdx int
	preRowIdx int

	nextLeft  types.Datetime
	nextRight types.Datetime
	// zeroWindow marks the dedicated bucket for MySQL's 0000-00-00 temporal
	// sentinel. It must not enter regular modulo/arithmetic window math, where
	// its internal value (-1) would alias the valid 0001-01-01 epoch (0).
	zeroWindow bool

	withoutFill bool

	last    bool
	lastVal types.Datetime

	// partIdx / partRow locate the row whose partition key the window
	// currently being accumulated belongs to; -1 before the first window.
	partIdx int
	partRow int
	// partEnd mirrors `end` but for one partition: its rows are exhausted, so
	// windows keep sliding until they pass the partition's last value.
	partEnd bool
	// breakVecIdx / breakRowIdx hold the first row of the next partition, the
	// row the restart re-anchors on.
	breakVecIdx int
	breakRowIdx int
	// partLast* track the last row seen inside the current partition, playing
	// the role `lastVal` and the final buffered row play for the whole stream.
	partLastVal    types.Datetime
	partLastVecIdx int
	partLastRowIdx int
	// partitionBreak marks that the pending flush ends a partition, so the
	// next window must restart rather than slide.
	partitionBreak   bool
	partitionWindows int64
	partitionCount   int64
	gapFillWindows   int64
}

type TimeWin struct {
	ctr container

	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	// PartitionBy holds the GROUP BY keys other than the window's timestamp.
	// Each distinct key value gets its own window sequence, so input must
	// arrive ordered by these keys first.
	PartitionBy []*plan.Expr

	TsType  plan.Type
	Ts      *plan.Expr
	EndExpr *plan.Expr
	// GapFillStart / GapFillEnd define an optional half-open output domain.
	// The planner sets them only as a pair after proving simple query bounds.
	GapFillStart *plan.Expr
	GapFillEnd   *plan.Expr

	Interval types.Datetime
	Sliding  types.Datetime

	WStart  bool
	WEnd    bool
	GapFill bool

	vm.OperatorBase
}

func (timeWin *TimeWin) GetOperatorBase() *vm.OperatorBase {
	return &timeWin.OperatorBase
}

func init() {
	reuse.CreatePool[TimeWin](
		func() *TimeWin {
			return &TimeWin{}
		},
		func(a *TimeWin) {
			*a = TimeWin{}
		},
		reuse.DefaultOptions[TimeWin]().
			WithEnableChecker(),
	)
}

func (timeWin TimeWin) TypeName() string {
	return opName
}

func NewArgument() *TimeWin {
	return reuse.Alloc[TimeWin](nil)
}

func (timeWin *TimeWin) Release() {
	if timeWin != nil {
		reuse.Free[TimeWin](timeWin, nil)
	}
}

func (timeWin *TimeWin) hasGapFillBounds() bool {
	return timeWin.GapFill && timeWin.GapFillStart != nil && timeWin.GapFillEnd != nil
}

func (ctr *container) hasGapFillBounds(timeWin *TimeWin) bool {
	return ctr.boundedGapFill && timeWin.hasGapFillBounds()
}

func (timeWin *TimeWin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.resetExes()
	releaseInheritedAccount := ctr.hasAccountedBufferedVector()
	// The last flushed batch and the aggregate executors belong to the finished
	// generation. In the sliding path the batch owns its aggregate prefix (the
	// boundaries belong to their expression executors and the partition keys to
	// partOut); in the interval path every vector is a buffer that outlives the
	// batch, so only the reference is dropped. Aggregates cannot be rewound
	// once Flush has run (see makeAggExecutors), so they are discarded here and
	// rebuilt by Prepare. Unaccounted tsVec/aggVec/partVec buffers stay
	// allocated: with the cursors back at zero the next generation reuses them
	// from index 0. Account-owned buffers are released below because their
	// lifetime cannot cross a statement-attempt generation.
	if timeWin.EndExpr == nil {
		ctr.freeFlushedAggVecs(proc.Mp())
	}
	ctr.bat = nil
	if releaseInheritedAccount {
		// Dup preserves the source vector's allocation selection. An upstream
		// accounted Order/MergeOrder can therefore charge these reusable input
		// caches to the current statement attempt. Such backing must not cross a
		// prepared-statement generation; unaccounted caches retain legacy reuse.
		ctr.freeVector(proc.Mp())
	}
	ctr.freeAgg()
	ctr.aggs = nil
	ctr.resetParam(timeWin)
	ctr.prepareParamKind.Reset(nil)
}

func (timeWin *TimeWin) MakeIntervalAndSliding(interval, sliding *plan.Expr) error {
	str := interval.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	typ, err := types.IntervalTypeOf(str)
	if err != nil {
		return err
	}
	val1 := interval.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
	if val1 <= 0 {
		return moerr.NewInvalidInputNoCtx("time window interval must be greater than zero")
	}
	timeWin.Interval, err = calcDatetime(val1, typ)
	if err != nil {
		return err
	}

	if sliding != nil {
		str = sliding.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		typ, err = types.IntervalTypeOf(str)
		if err != nil {
			return err
		}
		val2 := sliding.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
		if val2 <= 0 {
			return moerr.NewInvalidInputNoCtx("time window sliding value must be greater than zero")
		}
		timeWin.Sliding, err = calcDatetime(val2, typ)
		if err != nil {
			return err
		}
	}

	return nil
}

func calcDatetime(diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	var num int64
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	switch iTyp {
	case types.MicroSecond:
		num = diff
	case types.Second:
		num = diff * types.MicroSecsPerSec
	case types.Minute:
		num = diff * types.SecsPerMinute * types.MicroSecsPerSec
	case types.Hour:
		num = diff * types.SecsPerHour * types.MicroSecsPerSec
	case types.Day:
		num = diff * types.SecsPerDay * types.MicroSecsPerSec
	default:
		return 0, moerr.NewNotSupportedNoCtx("Time Window aggregate only support MICROSECOND, SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	return types.Datetime(num), nil
}

func (timeWin *TimeWin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.freeBatch(proc.Mp())
	ctr.freeVector(proc.Mp())
	ctr.freeExes()
	ctr.freeAgg()
	ctr.prepareParamKind.Reset(nil)
}

func (timeWin *TimeWin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetExes() {
	for _, exe := range ctr.aggExe {
		exe.ResetForNextQuery()
	}
	for _, exe := range ctr.partExe {
		if exe != nil {
			exe.ResetForNextQuery()
		}
	}
	if ctr.tsExe != nil {
		ctr.tsExe.ResetForNextQuery()
	}
	if ctr.startExe != nil {
		ctr.startExe.ResetForNextQuery()
	}
	if ctr.endExe != nil {
		ctr.endExe.ResetForNextQuery()
	}
	if ctr.gapFillStartExe != nil {
		ctr.gapFillStartExe.ResetForNextQuery()
	}
	if ctr.gapFillEndExe != nil {
		ctr.gapFillEndExe.ResetForNextQuery()
	}
}

// resetParam rewinds every piece of per-generation state, so a Reset/Prepare
// cycle starts from the same blank slate as a fresh operator. Any cursor left
// over from the previous run would either read stale buffered rows or, worse,
// route receive into nextWindow with window bounds that no longer exist.
func (ctr *container) resetParam(timeWin *TimeWin) {
	if timeWin.EndExpr != nil {
		ctr.status = interval
	} else {
		ctr.status = receive
	}
	ctr.i = 0
	ctr.end = false
	ctr.group = -1
	ctr.wStart = nil
	ctr.wEnd = nil

	ctr.curVecIdx = 0
	ctr.curRowIdx = 0
	ctr.preVecIdx = 0
	ctr.preRowIdx = 0
	ctr.left = 0
	ctr.right = 0
	ctr.nextLeft = 0
	ctr.nextRight = 0
	ctr.zeroWindow = false
	ctr.withoutFill = false
	ctr.last = false
	ctr.lastVal = 0

	ctr.partIdx = -1
	ctr.partRow = 0
	ctr.partEnd = false
	ctr.breakVecIdx = 0
	ctr.breakRowIdx = 0
	ctr.partLastVal = 0
	ctr.partLastVecIdx = 0
	ctr.partLastRowIdx = 0
	ctr.partitionBreak = false
	ctr.partitionWindows = 0
	ctr.partitionCount = 0
	ctr.gapFillWindows = 0
	ctr.gapFillStart = 0
	ctr.gapFillEnd = 0
	ctr.gapFillRows = 0
	ctr.boundedGapFill = false
	ctr.syntheticBounds = false
}

func (ctr *container) freeExes() {
	for _, exe := range ctr.aggExe {
		exe.Free()
	}
	for _, exe := range ctr.partExe {
		if exe != nil {
			exe.Free()
		}
	}
	if ctr.tsExe != nil {
		ctr.tsExe.Free()
	}
	if ctr.startExe != nil {
		ctr.startExe.Free()
	}
	if ctr.endExe != nil {
		ctr.endExe.Free()
	}
	if ctr.gapFillStartExe != nil {
		ctr.gapFillStartExe.Free()
	}
	if ctr.gapFillEndExe != nil {
		ctr.gapFillEndExe.Free()
	}
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
	}
}

func (ctr *container) freeAgg() {
	for _, a := range ctr.aggs {
		if a != nil {
			a.Free()
		}
	}
}

func (ctr *container) freeVector(mp *mpool.MPool) {
	for _, vec := range ctr.tsVec {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.tsVec = nil

	for _, aggregateVecs := range ctr.aggVec {
		for _, vecs := range aggregateVecs {
			for _, vec := range vecs {
				if vec != nil {
					vec.Free(mp)
				}
			}
		}
	}
	ctr.aggVec = nil

	for _, vecs := range ctr.partVec {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(mp)
			}
		}
	}
	ctr.partVec = nil

	ctr.freePartOut(mp)

	// calRes only ever hands the *cast results* of these two to the output
	// batch; the datetime staging vectors themselves are owned here and were
	// never released before.
	if ctr.startVec != nil {
		ctr.startVec.Free(mp)
		ctr.startVec = nil
	}
	if ctr.endVec != nil {
		ctr.endVec.Free(mp)
		ctr.endVec = nil
	}
}

func (ctr *container) hasAccountedBufferedVector() bool {
	if ctr == nil {
		return false
	}
	hasAccount := func(vec *vector.Vector) bool {
		return vec != nil && vec.AllocationAccountSelection() != nil
	}
	for _, vec := range ctr.tsVec {
		if hasAccount(vec) {
			return true
		}
	}
	for _, aggregateVecs := range ctr.aggVec {
		for _, vecs := range aggregateVecs {
			for _, vec := range vecs {
				if hasAccount(vec) {
					return true
				}
			}
		}
	}
	for _, vecs := range ctr.partVec {
		for _, vec := range vecs {
			if hasAccount(vec) {
				return true
			}
		}
	}
	for _, vec := range ctr.partOut {
		if hasAccount(vec) {
			return true
		}
	}
	return hasAccount(ctr.startVec) || hasAccount(ctr.endVec)
}

func (ctr *container) freePartOut(mp *mpool.MPool) {
	for _, vec := range ctr.partOut {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.partOut = nil
}
