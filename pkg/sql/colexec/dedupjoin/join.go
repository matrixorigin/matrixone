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
	"time"

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

const opName = "dedup_join"

func (dedupJoin *DedupJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": dedup join ")
}

func (dedupJoin *DedupJoin) OpType() vm.OpType {
	return vm.DedupJoin
}

func (dedupJoin *DedupJoin) Prepare(proc *process.Process) (err error) {
	if len(dedupJoin.ctr.tmpBatches) == 0 {
		dedupJoin.ctr.vecs = make([]*vector.Vector, len(dedupJoin.Conditions[0]))
		dedupJoin.ctr.evecs = make([]evalVector, len(dedupJoin.Conditions[0]))
		for i := range dedupJoin.ctr.evecs {
			dedupJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, dedupJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
		dedupJoin.ctr.tmpBatches = make([]*batch.Batch, 2)
	}

	dedupJoin.ctr.InitProc(proc)
	return err
}

func (dedupJoin *DedupJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(dedupJoin.GetIdx(), dedupJoin.GetParallelIdx(), dedupJoin.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()
	ctr := &dedupJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			dedupJoin.build(analyze, proc)

			if ctr.mp == nil && !dedupJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
			result, err = dedupJoin.Children[0].Call(proc)
			if err != nil {
				return result, err
			}
			bat := result.Batch
			if bat == nil {
				ctr.state = SendResult
				dedupJoin.ctr.buf = nil
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			if ctr.batchRowCount == 0 {
				continue
			}

			if err := ctr.probe(bat, dedupJoin, proc, analyze, dedupJoin.GetIsFirst(), dedupJoin.GetIsLast()); err != nil {
				return result, err
			}

			continue

		case SendResult:
			if dedupJoin.ctr.buf == nil {
				dedupJoin.ctr.lastpos = 0
				setNil, err := ctr.sendResult(dedupJoin, proc, analyze, dedupJoin.GetIsFirst(), dedupJoin.GetIsLast())
				if err != nil {
					return result, err
				}
				if setNil {
					ctr.state = End
				}
				continue
			} else {
				if dedupJoin.ctr.lastpos >= len(dedupJoin.ctr.buf) {
					ctr.state = End
					continue
				}
				result.Batch = dedupJoin.ctr.buf[dedupJoin.ctr.lastpos]
				dedupJoin.ctr.lastpos++
				result.Status = vm.ExecHasMore
				return result, nil
			}

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (dedupJoin *DedupJoin) build(anal process.Analyze, proc *process.Process) {
	ctr := &dedupJoin.ctr
	start := time.Now()
	defer anal.WaitStop(start)
	ctr.mp = message.ReceiveJoinMap(dedupJoin.JoinMapTag, dedupJoin.IsShuffle, dedupJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) sendResult(ap *DedupJoin, proc *process.Process, analyze process.Analyze, _ bool, isLast bool) (bool, error) {
	ctr.handledLast = true

	if ctr.matched.Count() == 0 {
		ap.ctr.buf = ctr.batches
		ctr.batches = nil
		for k := range ap.ctr.buf {
			analyze.Output(ap.ctr.buf[k], isLast)
		}
		return false, nil
	}

	count := ctr.batchRowCount - int64(ctr.matched.Count())
	ctr.matched.Negate()
	sels := make([]int32, 0, count)
	itr := ctr.matched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	if len(sels) <= colexec.DefaultBatchSize {
		if ctr.rbat != nil {
			ctr.rbat.CleanOnlyData()
		} else {
			ctr.rbat = batch.NewWithSize(len(ap.Result))

			for i, pos := range ap.Result {
				ctr.rbat.Vecs[i] = vector.NewVec(ap.RightTypes[pos])
			}
		}

		for j, pos := range ap.Result {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
					return false, err
				}
			}
		}
		ctr.rbat.AddRowCount(len(sels))
		analyze.Output(ctr.rbat, isLast)
		ap.ctr.buf = []*batch.Batch{ctr.rbat}
		return false, nil
	} else {
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
			for j, pos := range ap.Result {
				for _, sel := range newsels {
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					if err := ap.ctr.buf[k].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
						return false, err
					}
				}
			}
			ap.ctr.buf[k].SetRowCount(len(newsels))
			analyze.Output(ap.ctr.buf[k], isLast)
		}
		return false, nil
	}

}

func (ctr *container) probe(bat *batch.Batch, ap *DedupJoin, proc *process.Process, analyze process.Analyze, isFirst bool, _ bool) error {
	analyze.Input(bat, isFirst)

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	count := bat.RowCount()
	itr := ctr.mp.NewIterator()
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

			switch ap.OnDupAction {
			case plan.Node_ERROR:
				return moerr.NewDuplicateEntry(proc.Ctx, ctr.vecs[0].RowToString(i), "")
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
