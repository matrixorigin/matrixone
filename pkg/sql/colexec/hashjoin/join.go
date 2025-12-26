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

package hashjoin

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_join"

func (hashJoin *HashJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch hashJoin.JoinType {
	case plan.Node_INNER:
		buf.WriteString(": inner join ")
	case plan.Node_LEFT:
		buf.WriteString(": left join ")
	case plan.Node_RIGHT:
		buf.WriteString(": right join ")
	case plan.Node_SEMI:
		if hashJoin.IsRightJoin {
			buf.WriteString(": right semi join ")
		} else {
			buf.WriteString(": semi join ")
		}
	case plan.Node_ANTI:
		if hashJoin.IsRightJoin {
			buf.WriteString(": right anti join ")
		} else {
			buf.WriteString(": anti join ")
		}
	case plan.Node_SINGLE:
		buf.WriteString(": single join ")
	}
}

func (hashJoin *HashJoin) OpType() vm.OpType {
	return vm.HashJoin
}

func (hashJoin *HashJoin) Prepare(proc *process.Process) (err error) {
	if hashJoin.OpAnalyzer == nil {
		hashJoin.OpAnalyzer = process.NewAnalyzer(hashJoin.GetIdx(), hashJoin.IsFirst, hashJoin.IsLast, opName)
	} else {
		hashJoin.OpAnalyzer.Reset()
	}

	if len(hashJoin.ctr.vecs) == 0 {
		hashJoin.ctr.vecs = make([]*vector.Vector, len(hashJoin.EqConds[0]))
		hashJoin.ctr.evecs = make([]evalVector, len(hashJoin.EqConds[0]))
		for i := range hashJoin.EqConds[0] {
			hashJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, hashJoin.EqConds[0][i])
			if err != nil {
				return err
			}
		}
		if hashJoin.NonEqCond != nil {
			hashJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, hashJoin.NonEqCond)
			if err != nil {
				return err
			}
		}
	}

	hashJoin.ctr.handledLast = false

	return err
}

func (hashJoin *HashJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashJoin.OpAnalyzer

	ctr := &hashJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = hashJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.mp == nil && !hashJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				if hashJoin.JoinType == plan.Node_INNER || hashJoin.JoinType == plan.Node_RIGHT || hashJoin.JoinType == plan.Node_SEMI {
					ctr.state = End
					continue
				}
			}

			ctr.state = Probe

		case Probe:
			if hashJoin.ctr.inbat == nil {
				result, err = vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				bat := result.Batch

				if bat == nil {
					if hashJoin.IsRightJoin && (hashJoin.JoinType == plan.Node_RIGHT || hashJoin.JoinType == plan.Node_SEMI || hashJoin.JoinType == plan.Node_ANTI) {
						ctr.state = Finalize
					} else {
						ctr.state = End
					}

					continue
				}
				if bat.IsEmpty() {
					continue
				}
				if ctr.mp == nil {
					continue
				}
				ctr.inbat = bat
				ctr.lastPos = 0
			}

			startRow := ctr.lastPos
			if err := ctr.probe(hashJoin, proc, analyzer, &result); err != nil {
				return result, err
			}
			if ctr.lastPos == 0 {
				ctr.inbat = nil
			} else if ctr.lastPos == startRow {
				return result, moerr.NewInternalErrorNoCtx("hash join hanging")
			}
			return result, nil

		case Finalize:
			err := ctr.finalize(hashJoin, proc, &result)
			if err != nil {
				return result, err
			}

			ctr.state = End
			if result.Batch == nil {
				continue
			}

			result.Status = vm.ExecNext
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (hashJoin *HashJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &hashJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(hashJoin.JoinMapTag, hashJoin.IsShuffle, hashJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()

	if hashJoin.IsRightJoin && (hashJoin.JoinType == plan.Node_RIGHT || hashJoin.JoinType == plan.Node_SEMI || hashJoin.JoinType == plan.Node_ANTI) {
		if ctr.batchRowCount > 0 {
			ctr.matched = &bitmap.Bitmap{}
			ctr.matched.InitWithSize(ctr.batchRowCount)
		}
	}

	return nil
}

func (ctr *container) finalize(ap *HashJoin, proc *process.Process, result *vm.CallResult) error {
	ctr.handledLast = true

	if ctr.matched == nil {
		result.Batch = nil
		return nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			result.Batch = nil
			return nil
		} else {
			for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, ap.Channel)
				if v != nil {
					ctr.matched.Or(v)
				} else {
					result.Batch = nil
					return nil
				}
			}
			close(ap.Channel)
		}
	}

	count := ctr.batchRowCount - int64(ctr.matched.Count())
	ctr.matched.Negate()
	sels := make([]int32, 0, count)
	itr := ctr.matched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	ap.resetRBat()
	if err := ctr.rbat.PreExtend(proc.Mp(), len(sels)); err != nil {
		return err
	}

	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := vector.AppendMultiFixed(ctr.rbat.Vecs[i], 0, true, int(count), proc.Mp()); err != nil {
				return err
			}
		} else {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[i].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
					return err
				}
			}
		}
	}

	ctr.rbat.AddRowCount(len(sels))
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(ap *HashJoin, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	ap.resetRBat()

	if err := ctr.evalJoinCondition(ap.ctr.inbat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.inbat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	count := ap.ctr.inbat.RowCount()
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	itr := ctr.itr

	rowCntInc := 0
	for i := ap.ctr.lastPos; i < count; i += hashmap.UnitLimit {
		if rowCntInc >= colexec.DefaultBatchSize {
			ctr.rbat.AddRowCount(rowCntInc)
			result.Batch = ctr.rbat
			ap.ctr.lastPos = i
			return nil
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			if ap.HashOnPK || ctr.mp.HashOnUnique() {
				idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
				if ap.NonEqCond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.inbat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedColWithTypeCheck[bool](vec)
					if bs[0] {
						for j, rp := range ap.ResultCols {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
									return err
								}
							}
						}
						ctr.matched.Add(vals[k] - 1)
						rowCntInc++
					}
				} else {
					for j, rp := range ap.ResultCols {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
								return err
							}
						}
					}
					ctr.matched.Add(vals[k] - 1)
					rowCntInc++
				}
			} else {
				sels := ctr.mp.GetSels(vals[k] - 1)
				if ap.NonEqCond != nil {
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.inbat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2),
							1, ctr.cfs2); err != nil {
							return err
						}
						vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
						if err != nil {
							return err
						}
						if vec.IsConstNull() || vec.GetNulls().Contains(0) {
							continue
						}
						bs := vector.MustFixedColWithTypeCheck[bool](vec)
						if !bs[0] {
							continue
						}
						for j, rp := range ap.ResultCols {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
						ctr.matched.Add(uint64(sel))
						rowCntInc++
					}
				} else {
					for j, rp := range ap.ResultCols {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
								return err
							}
						} else {
							for _, sel := range sels {
								idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
					}
					for _, sel := range sels {
						ctr.matched.Add(uint64(sel))
					}
					rowCntInc += len(sels)
				}
			}

		}
	}

	ctr.rbat.AddRowCount(rowCntInc)
	//anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.ctr.lastPos = 0
	return nil
}

func (ctr *container) emptyProbe(ap *HashJoin, proc *process.Process, result *vm.CallResult) error {

	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := vector.GetUnionAllFunction(
				*ctr.rbat.Vecs[i].GetType(),
				proc.Mp(),
			)(ctr.rbat.Vecs[i], ap.ctr.inbat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			ctr.rbat.Vecs[i].SetClass(vector.CONSTANT)
			ctr.rbat.Vecs[i].SetLength(ctr.inbat.RowCount())
		}
	}
	ctr.rbat.AddRowCount(ap.ctr.inbat.RowCount())
	result.Batch = ctr.rbat
	ap.ctr.lastRow = 0
	ap.ctr.inbat = nil
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

func (hashJoin *HashJoin) resetRBat() {
	ctr := &hashJoin.ctr
	if ctr.rbat != nil {
		ctr.rbat.CleanOnlyData()
	} else {
		ctr.rbat = batch.NewWithSize(len(hashJoin.ResultCols))

		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.LeftTypes[rp.Pos])
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[rp.Pos])
			}
		}
	}
}
