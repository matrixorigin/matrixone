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

	return err
}

func (dedupJoin *DedupJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := dedupJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

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

			if err := ctr.probe(bat, dedupJoin, proc, analyzer); err != nil {
				return result, err
			}

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
			analyzer.Output(result.Batch)
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
		ctr.matched.InitWithSize(ctr.batchRowCount)
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

	if ctr.matched.Count() == 0 {
		ap.ctr.buf = ctr.batches
		ctr.batches = nil
		return nil
	}

	count := ctr.batchRowCount - int64(ctr.matched.Count())
	ctr.matched.Negate()
	sels := make([]int32, 0, count)
	itr := ctr.matched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	n := (len(sels)-1)/colexec.DefaultBatchSize + 1
	ap.ctr.buf = make([]*batch.Batch, n)
	for k := range ap.ctr.buf {
		ap.ctr.buf[k] = batch.NewWithSize(len(ap.Result))
		for i, pos := range ap.Result {
			ap.ctr.buf[k].Vecs[i] = vector.NewVec(ap.RightTypes[pos])
		}
		var newsels []int32
		if (k+1)*colexec.DefaultBatchSize <= len(sels) {
			newsels = sels[k*colexec.DefaultBatchSize : (k+1)*colexec.DefaultBatchSize]
		} else {
			newsels = sels[k*colexec.DefaultBatchSize:]
		}
		for _, sel := range newsels {
			idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
			for j, pos := range ap.Result {
				if err := ap.ctr.buf[k].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
					return err
				}
			}
		}
		ap.ctr.buf[k].SetRowCount(len(newsels))
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *DedupJoin, proc *process.Process, analyzer process.Analyzer) error {
	//analyzer.Input(bat)

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
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
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				continue
			}

			switch ap.OnDuplicateAction {
			case plan.Node_ERROR:
				// do nothing for txn.mode = Optimistic
				if isPessimistic {
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
			case plan.Node_IGNORE:
				ctr.matched.Add(vals[k] - 1)
			case plan.Node_UPDATE: // TODO
			}
		}
	}

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
