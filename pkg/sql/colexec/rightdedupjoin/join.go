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

package rightdedupjoin

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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "right_dedup_join"

func (rightDedupJoin *RightDedupJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": right dedup join ")
}

func (rightDedupJoin *RightDedupJoin) OpType() vm.OpType {
	return vm.RightDedupJoin
}

func (rightDedupJoin *RightDedupJoin) Prepare(proc *process.Process) (err error) {
	if rightDedupJoin.OpAnalyzer == nil {
		rightDedupJoin.OpAnalyzer = process.NewAnalyzer(rightDedupJoin.GetIdx(), rightDedupJoin.IsFirst, rightDedupJoin.IsLast, "dedup join")
	} else {
		rightDedupJoin.OpAnalyzer.Reset()
	}
	rightDedupJoin.ctr.spillThreshold = colexec.ResolveSpillThreshold(rightDedupJoin.SpillThreshold)

	newEvalVectors := len(rightDedupJoin.ctr.vecs) == 0
	newUpdateExecs := len(rightDedupJoin.ctr.exprExecs) == 0 && len(rightDedupJoin.UpdateColExprList) > 0
	var evalExecs, updateExecs []colexec.ExpressionExecutor
	if newEvalVectors {
		evalExecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, rightDedupJoin.Conditions[0])
		if err != nil {
			return err
		}
	}
	if newUpdateExecs {
		updateExecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, rightDedupJoin.UpdateColExprList)
		if err != nil {
			for _, exec := range evalExecs {
				exec.Free()
			}
			return err
		}
	}
	if newEvalVectors {
		evecs := make([]evalVector, len(evalExecs))
		for i := range evalExecs {
			evecs[i].executor = evalExecs[i]
		}
		rightDedupJoin.ctr.vecs = make([]*vector.Vector, len(evalExecs))
		rightDedupJoin.ctr.evecs = evecs
	}
	if newUpdateExecs {
		rightDedupJoin.ctr.exprExecs = updateExecs
	}

	return err
}

func (rightDedupJoin *RightDedupJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := rightDedupJoin.OpAnalyzer
	ctr := &rightDedupJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = rightDedupJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}
			if ctr.mp == nil && ctr.spillEngine == nil {
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			var bat *batch.Batch
			// Spill-read mode: read probe batches from engine.
			if ctr.spillEngine != nil && ctr.spillEngine.IsProbing() {
				var readErr error
				bat, readErr = ctr.spillEngine.NextProbeBatch(proc)
				if readErr != nil {
					return result, readErr
				}
				if bat == nil {
					ctr.spillEngine.FinishBucket()
					ctr.state = Finalize
					continue
				}
			} else if ctr.spillEngine != nil {
				ctr.state = Finalize
				continue
			} else {
				input, err = vm.ChildrenCall(rightDedupJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				bat = input.Batch
				if bat == nil {
					ctr.state = Finalize
					continue
				}
				if bat.IsEmpty() {
					continue
				}
			}
			if ctr.mp == nil {
				continue
			}
			if err := ctr.probe(bat, rightDedupJoin, proc, analyzer, &result); err != nil {
				return result, err
			}
			return result, nil
		case Finalize:
			if ctr.spillEngine != nil {
				// Clear previous bucket state before advancing.
				ctr.cleanHashMap()
				ctr.matched = nil
				ctr.groupCount = 0
				ctr.buildGroupCount = 0
				var initErr error
				ok, bktErr := ctr.spillEngine.AdvanceToNextBucket(proc, analyzer,
					func(jm *message.JoinMap, res spillutil.BucketResult) {
						switch res {
						case spillutil.BucketReady:
							ctr.mp = jm
							ctr.groupCount = jm.GetGroupCount()
							ctr.buildGroupCount = ctr.groupCount
							if !proc.GetTxnOperator().Txn().IsPessimistic() && ctr.buildGroupCount > 0 {
								ctr.matched = &bitmap.Bitmap{}
								ctr.matched.InitWithSize(int64(ctr.buildGroupCount))
							}
						case spillutil.BucketEmptyBuild:
							ctr.mp, initErr = rightDedupJoin.newEmptyJoinMap(proc)
						}
					})
				if bktErr != nil {
					return result, bktErr
				}
				if initErr != nil {
					return result, initErr
				}
				if ok {
					ctr.state = Probe
					continue
				}
			}
			ctr.state = End
			continue
		default:
			if ctr.mp != nil {
				ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (rightDedupJoin *RightDedupJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &rightDedupJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(rightDedupJoin.JoinMapTag, rightDedupJoin.IsShuffle, rightDedupJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return
	}

	if ctr.mp == nil {
		ctr.mp, err = rightDedupJoin.newEmptyJoinMap(proc)
		ctr.groupCount = 0
		ctr.buildGroupCount = 0
		ctr.matched = nil
		return err
	} else {
		// Handle spilled build side.
		if ctr.mp.IsSpilled() {
			engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
				BuildKeyExprs:           rightDedupJoin.Conditions[1],
				SpillThreshold:          ctr.spillThreshold,
				NeedsProbeForEmptyBuild: true,
				NeedsBuildForEmptyProbe: false,
				IsDedup:                 false,
				OnDuplicateAction:       rightDedupJoin.OnDuplicateAction,
				DedupColName:            rightDedupJoin.DedupColName,
				DedupColTypes:           rightDedupJoin.DedupColTypes,
				DelColIdx:               rightDedupJoin.DelColIdx,
			})
			engine.InitFromSpilledMap(ctr.mp.TakeSpillBuildFds())
			if err := engine.ScatterProbeTable(proc,
				func() (*batch.Batch, error) {
					input, err := vm.ChildrenCall(rightDedupJoin.GetChildren(0), proc, analyzer)
					return input.Batch, err
				},
				analyzer,
				func(bat *batch.Batch) ([]*vector.Vector, error) {
					if err := ctr.evalJoinCondition(bat, proc); err != nil {
						return nil, err
					}
					return ctr.vecs, nil
				},
			); err != nil {
				ctr.mp.Free()
				ctr.mp = nil
				engine.Cleanup(proc)
				return err
			}
			ctr.mp.Free()
			ctr.spillEngine = engine
			ctr.mp = nil
			return
		}
		ctr.groupCount = ctr.mp.GetGroupCount()
		ctr.buildGroupCount = ctr.groupCount
		if !proc.GetTxnOperator().Txn().IsPessimistic() {
			ctr.matched = &bitmap.Bitmap{}
			ctr.matched.InitWithSize(int64(ctr.buildGroupCount))
		}
	}

	return
}

func (rightDedupJoin *RightDedupJoin) newEmptyJoinMap(proc *process.Process) (*message.JoinMap, error) {
	keyWidth := 0
	for _, expr := range rightDedupJoin.Conditions[0] {
		typ := types.T(expr.Typ.Id)
		width := typ.TypeLen()
		if typ.FixedLength() < 0 {
			width = 128
		}
		keyWidth += width
	}

	var (
		intHashMap *hashmap.IntHashMap
		strHashMap *hashmap.StrHashMap
		err        error
	)
	if keyWidth <= 8 {
		intHashMap, err = hashmap.NewIntHashMap(false, proc.Mp())
	} else {
		strHashMap, err = hashmap.NewStrHashMap(false, proc.Mp())
	}
	if err != nil {
		return nil, err
	}

	jm := message.NewJoinMap(message.GroupSels{}, intHashMap, strHashMap, nil, nil, proc.Mp())
	jm.IncRef(1)
	return jm, nil
}

func (ctr *container) probe(bat *batch.Batch, ap *RightDedupJoin, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	err := ctr.evalJoinCondition(bat, proc)
	if err != nil {
		return err
	}

	count := bat.RowCount()
	err = ctr.mp.PreAlloc(uint64(count))
	if err != nil {
		return err
	}

	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}

	isPessimistic := proc.GetTxnOperator().Txn().IsPessimistic()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := min(count-i, hashmap.UnitLimit)
		vals, zvals, err := ctr.itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}

		for k, v := range vals[:n] {
			if zvals[k] == 0 || v == 0 {
				continue
			}

			switch ap.OnDuplicateAction {
			case plan.Node_FAIL:
				reportDup := false
				if isPessimistic {
					reportDup = v <= ctr.groupCount
				} else {
					if v <= ctr.buildGroupCount {
						if ctr.matched.Contains(v - 1) {
							reportDup = true
						} else {
							ctr.matched.Add(v - 1)
						}
					} else {
						reportDup = v <= ctr.groupCount
					}
				}

				if reportDup {
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
				}

				if v > ctr.groupCount {
					ctr.groupCount = v
				}

			case plan.Node_IGNORE:
				// TODO
				return moerr.NewNotSupported(proc.Ctx, "RIGHT DEDUP join not support IGNORE")

			case plan.Node_UPDATE:
				// TODO
				return moerr.NewNotSupported(proc.Ctx, "RIGHT DEDUP join not support UPDATE")
			}
		}
	}

	ctr.resetResultBatch()
	if ctr.resultBatch == nil {
		ctr.resultBatch = batch.NewOffHeapWithSize(len(ap.Result))
		for i, rp := range ap.Result {
			if rp.Rel == 0 {
				ctr.resultBatch.Vecs[i] = vector.NewOffHeapVecWithType(ap.LeftTypes[rp.Pos])
			} else {
				ctr.resultBatch.Vecs[i] = vector.NewOffHeapVecWithType(ap.RightTypes[rp.Pos])
			}
		}
	}

	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := ctr.resultBatch.Vecs[i].UnionBatch(bat.Vecs[rp.Pos], 0, count, nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			if err := vector.SetConstNull(ctr.resultBatch.Vecs[i], count, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.resultBatch.SetRowCount(count)
	result.Batch = ctr.resultBatch

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
