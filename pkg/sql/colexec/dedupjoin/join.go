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

package dedupjoin

import (
	"bytes"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "dedup_join"

func (dedupJoin *DedupJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": dedup join ")
}

func (dedupJoin *DedupJoin) OpType() vm.OpType {
	return vm.DedupJoin
}

func (dedupJoin *DedupJoin) Prepare(proc *process.Process) (err error) {
	if dedupJoin.OpAnalyzer == nil {
		dedupJoin.OpAnalyzer = process.NewAnalyzer(dedupJoin.GetIdx(), dedupJoin.IsFirst, dedupJoin.IsLast, "dedup join")
	} else {
		dedupJoin.OpAnalyzer.Reset()
	}

	if len(dedupJoin.ctr.vecs) == 0 {
		dedupJoin.ctr.vecs = make([]*vector.Vector, len(dedupJoin.Conditions[0]))
		dedupJoin.ctr.evecs = make([]evalVector, len(dedupJoin.Conditions[0]))
		for i := range dedupJoin.ctr.evecs {
			dedupJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, dedupJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
	}

	if len(dedupJoin.ctr.exprExecs) == 0 && len(dedupJoin.UpdateColExprList) > 0 {
		dedupJoin.ctr.exprExecs = make([]colexec.ExpressionExecutor, len(dedupJoin.UpdateColExprList))
		for i, expr := range dedupJoin.UpdateColExprList {
			dedupJoin.ctr.exprExecs[i], err = colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (dedupJoin *DedupJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := dedupJoin.OpAnalyzer

	ctr := &dedupJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = dedupJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.mp == nil && !dedupJoin.IsShuffle {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			result, err = vm.ChildrenCall(dedupJoin.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}

			bat := result.Batch
			if bat == nil {
				ctr.state = Finalize
				dedupJoin.ctr.buf = nil
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			if ctr.batchRowCount == 0 {
				continue
			}

			if err := ctr.probe(bat, dedupJoin, proc, analyzer, &result); err != nil {
				return result, err
			}

			return result, nil

		case Finalize:
			if dedupJoin.ctr.buf == nil {
				dedupJoin.ctr.lastPos = 0
				err := ctr.finalize(dedupJoin, proc)
				if err != nil {
					return result, err
				}
			}

			if dedupJoin.ctr.lastPos >= len(dedupJoin.ctr.buf) {
				ctr.state = End
				continue
			}

			result.Batch = dedupJoin.ctr.buf[dedupJoin.ctr.lastPos]
			dedupJoin.ctr.lastPos++
			result.Status = vm.ExecHasMore
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (dedupJoin *DedupJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &dedupJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(dedupJoin.JoinMapTag, dedupJoin.IsShuffle, dedupJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
	if ctr.batchRowCount > 0 {
		ctr.matched = &bitmap.Bitmap{}
		if dedupJoin.OnDuplicateAction != plan.Node_UPDATE {
			ctr.matched.InitWithSize(ctr.batchRowCount)
		} else {
			ctr.matched.InitWithSize(int64(ctr.mp.GetGroupCount()) + 1)
		}
	}
	return
}

func (ctr *container) finalize(ap *DedupJoin, proc *process.Process) error {
	ctr.handledLast = true

	if ctr.matched == nil {
		return nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			return nil
		} else {
			for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, ap.Channel)
				if v != nil {
					ctr.matched.Or(v)
				} else {
					return nil
				}
			}
			close(ap.Channel)
		}
	}

	if ap.OnDuplicateAction != plan.Node_UPDATE {
		if ctr.matched.Count() == 0 {
			ap.ctr.buf = ctr.batches
			ctr.batches = nil

			return nil
		}

		count := int(ctr.batchRowCount) - ctr.matched.Count()
		if count == 0 {
			return nil
		}

		ctr.matched.Negate()
		sels := make([]int32, 0, count)
		itr := ctr.matched.Iterator()
		for itr.HasNext() {
			r := itr.Next()
			sels = append(sels, int32(r))
		}

		batCnt := (count-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, batCnt)
		for i := range ap.ctr.buf {
			var newSels []int32
			if i+1 < batCnt {
				newSels = sels[i*colexec.DefaultBatchSize : (i+1)*colexec.DefaultBatchSize]
			} else {
				newSels = sels[i*colexec.DefaultBatchSize:]
			}

			ap.ctr.buf[i] = batch.NewWithSize(len(ap.Result))
			for j, rp := range ap.Result {
				if rp.Rel == 1 {
					ap.ctr.buf[i].Vecs[j] = vector.NewVec(ap.RightTypes[rp.Pos])
					for _, sel := range newSels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := ap.ctr.buf[i].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
							return err
						}
					}
				} else {
					ap.ctr.buf[i].Vecs[j] = vector.NewVec(ap.LeftTypes[rp.Pos])
					if err := vector.AppendMultiFixed(ap.ctr.buf[i].Vecs[j], 0, true, len(newSels), proc.Mp()); err != nil {
						return err
					}
				}
			}

			ap.ctr.buf[i].SetRowCount(len(newSels))
		}
	} else {
		sels := ctr.mp.GetSels(0)
		count := int(ctr.mp.GetGroupCount()) - ctr.matched.Count() + len(sels)
		if count == 0 {
			return nil
		}

		batCnt := (count-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, batCnt)

		fillCnt := 0
		batIdx, rowIdx := 0, 0
		for fillCnt < len(sels) {
			batSize := colexec.DefaultBatchSize
			if fillCnt+batSize > len(sels) {
				batSize = len(sels) - fillCnt
			}

			ap.ctr.buf[batIdx] = batch.NewWithSize(len(ap.Result))
			for i, rp := range ap.Result {
				if rp.Rel == 1 {
					ap.ctr.buf[batIdx].Vecs[i] = vector.NewVec(ap.RightTypes[rp.Pos])
					for _, sel := range sels[fillCnt : fillCnt+batSize] {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := ap.ctr.buf[batIdx].Vecs[i].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
							return err
						}
					}
				} else {
					ap.ctr.buf[batIdx].Vecs[i] = vector.NewVec(ap.LeftTypes[rp.Pos])
					if err := vector.AppendMultiFixed(ap.ctr.buf[batIdx].Vecs[i], 0, true, batSize, proc.Mp()); err != nil {
						return err
					}
				}
			}

			ap.ctr.buf[batIdx].SetRowCount(batSize)
			fillCnt += batSize
			batIdx++
			rowIdx = batSize % colexec.DefaultBatchSize
		}

		if ctr.joinBat1 != nil {
			ctr.joinBat1.Clean(proc.GetMPool())
		}
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())

		bitmapLen := uint64(ctr.matched.Len())
		for i := uint64(1); i < bitmapLen; i++ {
			if ctr.matched.Contains(i) {
				continue
			}

			if rowIdx == 0 {
				ap.ctr.buf[batIdx] = batch.NewWithSize(len(ap.Result))
				for i, rp := range ap.Result {
					if rp.Rel == 1 {
						ap.ctr.buf[batIdx].Vecs[i] = vector.NewVec(ap.RightTypes[rp.Pos])
					} else {
						ap.ctr.buf[batIdx].Vecs[i] = vector.NewVec(ap.LeftTypes[rp.Pos])
					}
				}
			}

			sels = ctr.mp.GetSels(i)
			idx1, idx2 := sels[0]/colexec.DefaultBatchSize, sels[0]%colexec.DefaultBatchSize
			err := colexec.SetJoinBatchValues(ctr.joinBat1, ctr.batches[idx1], int64(idx2), 1, ctr.cfs1)
			if err != nil {
				return err
			}

			for _, sel := range sels[1:] {
				idx1, idx2 = sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				err = colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2)
				if err != nil {
					return err
				}

				vecs := make([]*vector.Vector, len(ctr.exprExecs))
				for j, exprExec := range ctr.exprExecs {
					vecs[j], err = exprExec.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
					if err != nil {
						return err
					}
				}

				for j, pos := range ap.UpdateColIdxList {
					ctr.joinBat1.Vecs[pos] = vecs[j]
				}
			}

			for j, rp := range ap.Result {
				if rp.Rel == 1 {
					if err := ap.ctr.buf[batIdx].Vecs[j].UnionOne(ctr.joinBat1.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ap.ctr.buf[batIdx].Vecs[j].UnionNull(proc.Mp()); err != nil {
						return err
					}
				}
			}

			ap.ctr.buf[batIdx].AddRowCount(1)
			rowIdx++
			if rowIdx == colexec.DefaultBatchSize {
				batIdx++
				rowIdx = 0
			}
		}
	}

	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *DedupJoin, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	ap.resetRBat()

	err := ctr.evalJoinCondition(bat, proc)
	if err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	rowCntInc := 0
	count := bat.RowCount()
	itr := ctr.mp.NewIterator()
	isPessimistic := proc.GetTxnOperator().Txn().IsPessimistic()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				continue
			}

			switch ap.OnDuplicateAction {
			case plan.Node_FAIL:
				// do nothing for txn.mode = Optimistic
				if !isPessimistic {
					continue
				}

				var rowStr string
				if len(ap.DedupColTypes) == 1 {
					if ap.DedupColName == catalog.IndexTableIndexColName {
						if ctr.vecs[0].GetType().Oid == types.T_varchar {
							t, _, schema, err := types.DecodeTuple(ctr.vecs[0].GetBytesAt(i + k))
							if err == nil && len(schema) > 1 {
								rowStr = t.ErrString(make([]int32, len(schema)))
							}
						}
					}

					if len(rowStr) == 0 {
						rowStr = ctr.vecs[0].RowToString(i + k)
					}
				} else {
					rowItems, err := types.StringifyTuple(ctr.vecs[0].GetBytesAt(i+k), ap.DedupColTypes)
					if err != nil {
						return err
					}
					rowStr = "(" + strings.Join(rowItems, ",") + ")"
				}
				return moerr.NewDuplicateEntry(proc.Ctx, rowStr, ap.DedupColName)

			case plan.Node_IGNORE:
				ctr.matched.Add(vals[k] - 1)

			case plan.Node_UPDATE:
				err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k), 1, ctr.cfs1)
				if err != nil {
					return err
				}

				sels := ctr.mp.GetSels(vals[k])
				for _, sel := range sels {
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					err = colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2)
					if err != nil {
						return err
					}

					vecs := make([]*vector.Vector, len(ctr.exprExecs))
					for j, exprExec := range ctr.exprExecs {
						vecs[j], err = exprExec.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
						if err != nil {
							return err
						}
					}

					for j, pos := range ap.UpdateColIdxList {
						ctr.joinBat1.Vecs[pos] = vecs[j]
					}
				}

				for j, rp := range ap.Result {
					if rp.Rel == 1 {
						//if last index is row_id, meams need fetch right child's partition column
						//@FIXME should have better way to get right child's partition column
						var srcVec *vector.Vector
						if ctr.joinBat1.Vecs[rp.Pos].GetType().Oid == types.T_Rowid {
							srcVec = ctr.joinBat2.Vecs[rp.Pos]
						} else {
							srcVec = ctr.joinBat1.Vecs[rp.Pos]
						}
						if err := ctr.rbat.Vecs[j].UnionOne(srcVec, 0, proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
							return err
						}
					}
				}

				ctr.matched.Add(vals[k])
				rowCntInc++
			}
		}
	}

	ctr.rbat.AddRowCount(rowCntInc)
	result.Batch = ctr.rbat
	ap.ctr.lastPos = 0

	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}

func (dedupJoin *DedupJoin) resetRBat() {
	ctr := &dedupJoin.ctr
	if ctr.rbat != nil {
		ctr.rbat.CleanOnlyData()
	} else {
		ctr.rbat = batch.NewWithSize(len(dedupJoin.Result))

		for i, rp := range dedupJoin.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(dedupJoin.LeftTypes[rp.Pos])
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(dedupJoin.RightTypes[rp.Pos])
			}
		}
	}
}
