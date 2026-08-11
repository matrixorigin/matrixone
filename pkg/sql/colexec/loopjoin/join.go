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

package loopjoin

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_join"

func (loopJoin *LoopJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch loopJoin.JoinType {
	case plan.Node_INNER:
		buf.WriteString(": loop inner join ")
	case plan.Node_ANTI:
		buf.WriteString(": loop anti join ")
	case plan.Node_LEFT:
		buf.WriteString(": loop left join ")
	case plan.Node_MARK:
		buf.WriteString(": loop mark join ")
	case plan.Node_SEMI:
		buf.WriteString(": loop semi join ")
	case plan.Node_SINGLE:
		buf.WriteString(": loop single join ")
	case plan.Node_OUTER:
		buf.WriteString(": loop full outer join ")
	}
}

func (loopJoin *LoopJoin) OpType() vm.OpType {
	return vm.LoopJoin
}

func (loopJoin *LoopJoin) Prepare(proc *process.Process) error {
	var err error
	if loopJoin.allocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	loopJoin.recursiveProbe = false
	if loopJoin.ctr.resultBatchByteLimit <= 0 {
		loopJoin.ctr.resultBatchByteLimit = defaultLoopJoinResultBatchBytes
	}
	if loopJoin.NumChildren() > 0 {
		_ = vm.HandleAllOp(loopJoin.GetChildren(0), func(_ vm.Operator, op vm.Operator) error {
			if op.OpType() == vm.MergeRecursive {
				loopJoin.recursiveProbe = true
			}
			return nil
		})
	}
	if loopJoin.OpAnalyzer == nil {
		loopJoin.OpAnalyzer = process.NewAnalyzer(loopJoin.GetIdx(), loopJoin.IsFirst, loopJoin.IsLast, opName)
	} else {
		loopJoin.OpAnalyzer.Reset()
	}

	if loopJoin.NonEqCond != nil && loopJoin.ctr.expr == nil {
		var execs []colexec.ExpressionExecutor
		execs, err = hashbuild.NewExpressionExecutors(
			proc,
			[]*plan.Expr{loopJoin.NonEqCond},
			loopJoin.allocationAccount,
		)
		if err != nil {
			return err
		}
		loopJoin.ctr.expr = execs[0]
	}
	return err
}

func (loopJoin *LoopJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := loopJoin.OpAnalyzer

	ctr := &loopJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = loopJoin.build(proc, analyzer); err != nil {
				return result, err
			}
			if ctr.mp == nil && (loopJoin.JoinType == plan.Node_INNER || loopJoin.JoinType == plan.Node_SEMI) && !loopJoin.recursiveProbe {
				ctr.state = End
			} else {
				if loopJoin.JoinType == plan.Node_OUTER && ctr.mp != nil {
					if err = ctr.initRightMatchedBitmap(loopJoin, proc); err != nil {
						return result, err
					}
				}
				ctr.state = Probe
			}

		case Probe:
			if ctr.inBat == nil {
				input, err = vm.ChildrenCall(loopJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				if input.Batch == nil {
					if loopJoin.JoinType == plan.Node_OUTER && ctr.rightRowsMatched != nil {
						ctr.rightRowsMatched.Negate()
						ctr.rightMatchedIter = ctr.rightRowsMatched.Iterator()
						ctr.state = Finalize
						continue
					}
					ctr.state = End
					continue
				}
				if input.Batch.Last() {
					return input, nil
				}
				if input.Batch.IsEmpty() {
					continue
				}
				if loopJoin.recursiveProbe && ctr.mp == nil &&
					(loopJoin.JoinType == plan.Node_INNER || loopJoin.JoinType == plan.Node_SEMI) {
					continue
				}
				ctr.inBat = input.Batch
				ctr.probeIdx = 0
				ctr.batIdx = 0
				ctr.batRowIdx = 0
			}

			if err = loopJoin.resetResultBat(); err != nil {
				return result, err
			}
			for i, rp := range loopJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.resBat.Vecs[i].SetSorted(ctr.inBat.Vecs[rp.Pos].GetSorted())
				}
			}

			if ctr.mp == nil {
				err = ctr.emptyProbe(loopJoin, proc, &result)
			} else {
				err = ctr.probe(loopJoin, proc, &result)
			}

			if err != nil {
				return result, err
			}

			return result, err
		case Finalize:
			if err = ctr.finalize(loopJoin, proc, &result); err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.state = End
				continue
			}
			return result, nil
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopJoin *LoopJoin) build(proc *process.Process, analyzer process.Analyzer) (err error) {
	loopJoin.ctr.mp, err = process.MeasureWait(analyzer, resource.WaitOther, func() (*message.JoinMap, error) {
		return message.ReceiveJoinMap(loopJoin.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	})
	return err
}

func (ctr *container) emptyProbe(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	start := ctr.probeIdx
	count := ctr.emptyProbeChunk(ap, start)
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := ctr.resBat.Vecs[i].UnionBatch(
				ctr.inBat.Vecs[rp.Pos], int64(start), count, nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			switch ap.JoinType {
			case plan.Node_LEFT, plan.Node_SINGLE, plan.Node_OUTER:
				if err := vector.SetConstNull(ctr.resBat.Vecs[i], count, proc.Mp()); err != nil {
					return err
				}

			case plan.Node_MARK:
				err := vector.SetConstFixed(ctr.resBat.Vecs[i], false, count, proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	ctr.resBat.AddRowCount(count)
	result.Batch = ctr.resBat
	ctr.probeIdx += count
	if ctr.probeIdx >= ctr.inBat.RowCount() {
		ctr.inBat = nil
		ctr.probeIdx = 0
	}
	return nil
}

func (ctr *container) resultBatchFull(rows int) bool {
	return rows >= colexec.DefaultBatchSize ||
		rows > 0 && ctr.resBat.Size() >= ctr.resultBatchByteLimit
}

// canAppendRow is the single result-batch admission rule. A row may be added
// when both row and byte limits remain satisfied. The only exception is an
// intrinsically oversized first row: admitting it is required for progress,
// and resultBatchFull forces an immediate yield afterwards.
func (ctr *container) canAppendRow(rows, usedBytes, rowBytes int) bool {
	if rows >= colexec.DefaultBatchSize {
		return false
	}
	if rows == 0 {
		return true
	}
	return usedBytes <= ctr.resultBatchByteLimit &&
		rowBytes <= ctr.resultBatchByteLimit-usedBytes
}

func vectorLogicalRowBytes(vec *vector.Vector, row int) int {
	size := vec.GetType().TypeSize()
	if vec.GetType().IsVarlen() && !vec.IsNull(uint64(row)) {
		valueBytes := len(vec.GetBytesAt(row))
		if valueBytes > types.VarlenaInlineSize {
			size += valueBytes
		}
	}
	return size
}

func (ctr *container) emptyProbeRowBytes(ap *LoopJoin, row int) int {
	size := 0
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			size += vectorLogicalRowBytes(ctr.inBat.Vecs[rp.Pos], row)
		} else {
			size += ctr.resBat.Vecs[i].GetType().TypeSize()
		}
	}
	return size
}

func (ctr *container) emptyProbeChunk(ap *LoopJoin, start int) int {
	maxRows := min(colexec.DefaultBatchSize, ctr.inBat.RowCount()-start)
	used := 0
	for count := 0; count < maxRows; count++ {
		rowBytes := ctr.emptyProbeRowBytes(ap, start+count)
		if !ctr.canAppendRow(count, used, rowBytes) {
			return count
		}
		used += rowBytes
	}
	return maxRows
}

func (ctr *container) joinedRowBytes(
	ap *LoopJoin,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	buildRow int,
) int {
	size := 0
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			size += vectorLogicalRowBytes(inBat.Vecs[rp.Pos], probeRow)
		} else if rp.Rel == 1 {
			size += vectorLogicalRowBytes(buildBat.Vecs[rp.Pos], buildRow)
		} else {
			size += ctr.resBat.Vecs[i].GetType().TypeSize()
		}
	}
	return size
}

func (loopJoin *LoopJoin) resetResultBat() error {
	ctr := &loopJoin.ctr
	if ctr.resBat != nil {
		ctr.resBat.CleanOnlyData()
		for i := range ctr.resBat.Vecs {
			ctr.resBat.Vecs[i].SetClass(vector.FLAT)
			ctr.resBat.Vecs[i].SetLength(0)
		}
	} else {
		ctr.resBat = batch.NewOffHeapWithSize(len(loopJoin.ResultCols))

		for i, rp := range loopJoin.ResultCols {
			switch rp.Rel {
			case 0:
				var leftType types.Type
				if ctr.inBat != nil && int(rp.Pos) < len(ctr.inBat.Vecs) {
					leftType = *ctr.inBat.Vecs[rp.Pos].GetType()
				} else if int(rp.Pos) < len(loopJoin.LeftTypes) {
					leftType = loopJoin.LeftTypes[rp.Pos]
				} else {
					ctr.resBat.Clean(nil)
					ctr.resBat = nil
					return process.ErrExecutionResourceInvalid
				}
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(leftType)

			case 1:
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(loopJoin.RightTypes[rp.Pos])

			case -1:
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
			}
		}
		if err := ctr.resBat.SetAllocationAccount(loopJoin.resultAllocation); err != nil {
			ctr.resBat.Clean(nil)
			ctr.resBat = nil
			return err
		}
	}
	return nil
}

// initRightMatchedBitmap allocates the per-build-row matched bitmap.
func (ctr *container) initRightMatchedBitmap(
	ap *LoopJoin,
	proc *process.Process,
) error {
	bats := ctr.mp.GetBatches()
	var err error
	ctr.rightBatchOffset, err = mpool.MakeSliceAccounted[uint64](
		len(bats),
		proc.Mp(),
		ap.allocationAccount,
		hashbuild.HashBuildAllocationOwner,
		loopJoinAllocationSiteBatchOffsets,
	)
	if err != nil {
		return err
	}
	var total uint64
	for i, b := range bats {
		ctr.rightBatchOffset[i] = total
		total += uint64(b.RowCount())
	}
	if total > uint64(^uint64(0)>>1) {
		ctr.cleanRightMatchState(proc)
		return mpool.ErrAllocationAccountInvalid
	}
	ctr.rightRowsMatched, err = colexec.NewAccountedBitmap(
		int64(total),
		proc.Mp(),
		ap.allocationAccount,
		hashbuild.HashBuildAllocationOwner,
		loopJoinAllocationSiteMatched,
	)
	if err != nil {
		ctr.cleanRightMatchState(proc)
		return err
	}
	return nil
}

// finalize emits one batch worth of unmatched build rows with NULL probe
// columns. Iterator is monotonic, so rightMatchedBat only advances.
func (ctr *container) finalize(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	bats := ctr.mp.GetBatches()
	if err := ap.resetResultBat(); err != nil {
		return err
	}

	rowCnt := 0
	usedBytes := 0
	for ctr.rightPendingRow || ctr.rightMatchedIter.HasNext() {
		row := ctr.rightPending
		if !ctr.rightPendingRow {
			row = ctr.rightMatchedIter.Next()
			ctr.rightPending = row
			ctr.rightPendingRow = true
		}
		for ctr.rightMatchedBat+1 < len(ctr.rightBatchOffset) && ctr.rightBatchOffset[ctr.rightMatchedBat+1] <= row {
			ctr.rightMatchedBat++
		}
		j := int64(row - ctr.rightBatchOffset[ctr.rightMatchedBat])
		rowBytes := 0
		for i, rp := range ap.ResultCols {
			if rp.Rel == 1 {
				rowBytes += vectorLogicalRowBytes(
					bats[ctr.rightMatchedBat].Vecs[rp.Pos], int(j))
			} else {
				rowBytes += ctr.resBat.Vecs[i].GetType().TypeSize()
			}
		}
		if !ctr.canAppendRow(rowCnt, usedBytes, rowBytes) {
			break
		}
		for i, rp := range ap.ResultCols {
			if rp.Rel == 1 {
				if err := ctr.resBat.Vecs[i].UnionOne(bats[ctr.rightMatchedBat].Vecs[rp.Pos], j, proc.Mp()); err != nil {
					return err
				}
			}
		}
		ctr.rightPendingRow = false
		rowCnt++
		usedBytes += rowBytes
	}
	if rowCnt == 0 {
		result.Batch = nil
		return nil
	}
	for i, rp := range ap.ResultCols {
		if rp.Rel != 1 {
			if err := vector.AppendMultiFixed(ctr.resBat.Vecs[i], 0, true, rowCnt, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.resBat.AddRowCount(rowCnt)
	result.Batch = ctr.resBat
	return nil
}
