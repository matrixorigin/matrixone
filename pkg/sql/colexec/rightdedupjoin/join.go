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

	if len(rightDedupJoin.ctr.vecs) == 0 {
		rightDedupJoin.ctr.vecs = make([]*vector.Vector, len(rightDedupJoin.Conditions[0]))
		rightDedupJoin.ctr.evecs = make([]evalVector, len(rightDedupJoin.Conditions[0]))
		for i := range rightDedupJoin.ctr.evecs {
			rightDedupJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, rightDedupJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
	}

	if len(rightDedupJoin.ctr.exprExecs) == 0 && len(rightDedupJoin.UpdateColExprList) > 0 {
		rightDedupJoin.ctr.exprExecs = make([]colexec.ExpressionExecutor, len(rightDedupJoin.UpdateColExprList))
		for i, expr := range rightDedupJoin.UpdateColExprList {
			rightDedupJoin.ctr.exprExecs[i], err = colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return err
			}
		}
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

			ctr.state = Probe

		case Probe:
			input, err = vm.ChildrenCall(rightDedupJoin.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}

			bat := input.Batch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			if err := ctr.probe(bat, rightDedupJoin, proc, analyzer, &result); err != nil {
				return result, err
			}

			return result, nil

		default:
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
	if ctr.mp != nil {
		ctr.groupCount = ctr.mp.GetGroupCount()
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	return
}

func (ctr *container) probe(bat *batch.Batch, ap *RightDedupJoin, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	err := ctr.evalJoinCondition(bat, proc)
	if err != nil {
		return err
	}

	count := bat.RowCount()
	itr := ctr.mp.NewIterator()
	isPessimistic := proc.GetTxnOperator().Txn().IsPessimistic()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}

		for k, v := range vals[:n] {
			if zvals[k] == 0 || v == 0 {
				continue
			}

			switch ap.OnDuplicateAction {
			case plan.Node_FAIL:
				if v <= ctr.groupCount && isPessimistic {
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

				ctr.groupCount = v

			case plan.Node_IGNORE:
				// TODO
				return moerr.NewNotSupported(proc.Ctx, "RIGHT DEDUP join not support IGNORE")

			case plan.Node_UPDATE:
				// TODO
				return moerr.NewNotSupported(proc.Ctx, "RIGHT DEDUP join not support UPDATE")
			}
		}
	}

	result.Batch = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		result.Batch.Vecs[i], err = bat.Vecs[rp.Pos].Dup(proc.Mp())
		if err != nil {
			return err
		}
	}
	result.Batch.SetRowCount(count)

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
