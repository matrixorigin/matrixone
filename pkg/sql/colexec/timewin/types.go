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
)

type container struct {
	bat    *batch.Batch
	colCnt int
	i      int

	aggExe []colexec.ExpressionExecutor
	aggVec [][]*vector.Vector

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

	status int32
	end    bool

	group int
	aggs  []aggexec.AggFuncExec

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
	partitionBreak bool
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

	Interval types.Datetime
	Sliding  types.Datetime

	WStart bool
	WEnd   bool

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

func (timeWin *TimeWin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.resetExes()
	// The last flushed batch and the aggregate executors belong to the finished
	// generation. In the sliding path the batch owns its aggregate prefix (the
	// boundaries belong to their expression executors and the partition keys to
	// partOut); in the interval path every vector is a buffer that outlives the
	// batch, so only the reference is dropped. Aggregates cannot be rewound
	// once Flush has run (see makeAggExecutors), so they are discarded here and
	// rebuilt by Prepare. The tsVec/aggVec/partVec buffers stay allocated: with
	// the cursors back at zero the next generation reuses them from index 0.
	if timeWin.EndExpr == nil {
		ctr.freeFlushedAggVecs(proc.Mp())
	}
	ctr.bat = nil
	ctr.freeAgg()
	ctr.aggs = nil
	ctr.resetParam(timeWin)
}

func (timeWin *TimeWin) MakeIntervalAndSliding(interval, sliding *plan.Expr) error {
	str := interval.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	typ, err := types.IntervalTypeOf(str)
	if err != nil {
		return err
	}
	val1 := interval.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
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
	case types.Second:
		num = diff * types.MicroSecsPerSec
	case types.Minute:
		num = diff * types.SecsPerMinute * types.MicroSecsPerSec
	case types.Hour:
		num = diff * types.SecsPerHour * types.MicroSecsPerSec
	case types.Day:
		num = diff * types.SecsPerDay * types.MicroSecsPerSec
	default:
		return 0, moerr.NewNotSupportedNoCtx("Time Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	return types.Datetime(num), nil
}

func (timeWin *TimeWin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.freeBatch(proc.Mp())
	ctr.freeVector(proc.Mp())
	ctr.freeExes()
	ctr.freeAgg()
}

func (timeWin *TimeWin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetExes() {
	for _, exe := range ctr.aggExe {
		if exe != nil {
			exe.ResetForNextQuery()
		}
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
}

func (ctr *container) freeExes() {
	for _, exe := range ctr.aggExe {
		if exe != nil {
			exe.Free()
		}
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

	for _, vecs := range ctr.aggVec {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(mp)
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

func (ctr *container) freePartOut(mp *mpool.MPool) {
	for _, vec := range ctr.partOut {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.partOut = nil
}
