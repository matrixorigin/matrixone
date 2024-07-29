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

package mark

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "mark_join"

func (markJoin *MarkJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": mark join ")
}

func (markJoin *MarkJoin) OpType() vm.OpType {
	return vm.Mark
}

func (markJoin *MarkJoin) Prepare(proc *process.Process) error {
	var err error
	markJoin.ctr = new(container)
	markJoin.ctr.InitReceiver(proc, true)
	markJoin.ctr.evecs = make([]evalVector, len(markJoin.Conditions[0]))
	markJoin.ctr.vecs = make([]*vector.Vector, len(markJoin.Conditions[0]))
	markJoin.ctr.bat = batch.NewWithSize(len(markJoin.Typs))
	for i, typ := range markJoin.Typs {
		markJoin.ctr.bat.Vecs[i] = proc.GetVector(typ)
	}

	markJoin.ctr.buildEqVec = make([]*vector.Vector, len(markJoin.Conditions[1]))
	markJoin.ctr.buildEqEvecs = make([]evalVector, len(markJoin.Conditions[1]))

	if markJoin.Cond != nil {
		markJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, markJoin.Cond)
	}
	return err
}

// Note: before mark join, right table has been used in hashbuild operator to build JoinMap, which only contains those tuples without null
// the idx of tuples contains null is stored in nullSels

// 1. for each tuple in left table, join with tuple(s) in right table based on Three-valued logic. Conditions may contain equal conditions and non-equal conditions
// logic state for same row is Three-valued AND, for different rows is Three-valued OR

// 2.1 if a probe tuple has null(i.e. zvals[k] == 0)
//       scan whole right table directly and join with each tuple to determine state

// 2.2 if a probe tuple has no null. then scan JoinMap firstly to check equal condtions.(condEq)
//	    2.2.1 if condEq is condtrue in JoinMap(i.e. vals[k] > 0)
//	 		    further check non-eq condtions in those tupe IN JoinMap
//				2.2.1.1 if condNonEq is condTrue
//						   mark as condTrue
//	 	        2.2.1.2 if condNonEq is condUnkown
//						   mark as condUnkown
//	 	        2.2.1.3 if condNonEq is condFalse in JoinMap
//						   further check eq and non-eq conds IN nullSels
//                         (probe state could still be unknown BUT NOT FALSE as long as one unknown state exists, so have to scan the whole right table)

//	    2.2.2 if condEq is condFalse in JoinMap
//				check eq and non-eq conds in nullSels to determine condState. (same as 2.2.1.3)

func (markJoin *MarkJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(markJoin.GetIdx(), markJoin.GetParallelIdx(), markJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := markJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(markJoin, proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			msg := ctr.ReceiveFromAllRegs(anal)
			if msg.Err != nil {
				return result, msg.Err
			}

			bat := msg.Batch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}
			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				if err = ctr.emptyProbe(bat, markJoin, proc, anal, markJoin.GetIsFirst(), markJoin.GetIsLast(), &result); err != nil {
					bat.Clean(proc.Mp())
					result.Status = vm.ExecStop
					return result, err
				}
			} else {
				if err = ctr.probe(bat, markJoin, proc, anal, markJoin.GetIsFirst(), markJoin.GetIsLast(), &result); err != nil {
					bat.Clean(proc.Mp())
					result.Status = vm.ExecStop
					return result, err
				}
			}
			proc.PutBatch(bat)
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (markJoin *MarkJoin) receiveHashMap(anal process.Analyze, proc *process.Process) {
	ctr := markJoin.ctr
	ctr.mp = proc.ReceiveJoinMap(anal, markJoin.JoinMapTag, false, 0)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
}

func (ctr *container) receiveBatch(ap *MarkJoin, proc *process.Process, anal process.Analyze) error {
	msg := ctr.ReceiveFromSingleReg(1, anal)
	if msg.Err != nil {
		return msg.Err
	}
	bat := msg.Batch
	var err error
	if bat != nil {
		ctr.evalNullSels(bat)
		ctr.nullWithBatch, err = DumpBatch(bat, proc, ctr.nullSels)
		if err != nil {
			return err
		}
		if err = ctr.evalJoinBuildCondition(bat, proc); err != nil {
			return err
		}
		ctr.rewriteCond = colexec.RewriteFilterExprList(ap.OnList)
		if ctr.bat != nil {
			proc.PutBatch(ctr.bat)
			ctr.bat = nil
		}
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) build(ap *MarkJoin, proc *process.Process, anal process.Analyze) error {
	ap.receiveHashMap(anal, proc)
	return ctr.receiveBatch(ap, proc, anal)
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *MarkJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) (err error) {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	count := bat.RowCount()
	for i, rp := range ap.Result {
		if rp >= 0 {
			typ := *bat.Vecs[rp].GetType()
			ctr.rbat.Vecs[i] = proc.GetVector(typ)
			err = vector.GetUnionAllFunction(typ, proc.Mp())(ctr.rbat.Vecs[i], bat.Vecs[rp])
		} else {
			ctr.rbat.Vecs[i], err = vector.NewConstFixed(types.T_bool.ToType(), false, count, proc.Mp())
		}
		if err != nil {
			return err
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	anal.Output(ctr.rbat, isLast)

	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *MarkJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	markVec, err := proc.AllocVectorOfRows(types.T_bool.ToType(), bat.RowCount(), nil)
	if err != nil {
		return err
	}
	ctr.markVals = vector.MustFixedCol[bool](markVec)
	ctr.markNulls = nulls.NewWithSize(bat.RowCount())

	if err = ctr.evalJoinProbeCondition(bat, proc); err != nil {
		return err
	}

	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		var condState otyp
		// var condNonEq otyp
		// var condEq otyp
		var err error
		for k := 0; k < n; k++ {
			if zvals[k] == 0 { // 2.1 : probe tuple has null
				condState, err = ctr.EvalEntire(bat, ctr.bat, i+k, proc, ctr.rewriteCond)
				if err != nil {
					return err
				}
				ctr.handleResultType(i+k, condState)
			} else if vals[k] > 0 { // 2.2.1 : condEq is condTrue in JoinMap
				condState, err = ctr.nonEqJoinInMap(ap, mSels, vals, k, i, proc, bat)
				if err != nil {
					return err
				}
				if condState == condTrue { // 2.2.1.1 : condNonEq is condTrue in JoinMap
					ctr.markVals[i+k] = true
				} else if condState == condUnkown { // 2.2.1.2 : condNonEq is condUnkown in JoinMap
					nulls.Add(ctr.markNulls, uint64(i+k))
				} else { // 2.2.1.3 : condNonEq is condFalse in JoinMap, further check in nullSels
					if len(ctr.nullSels) == 0 {
						ctr.handleResultType(i+k, condFalse)
						continue
					}
					condState, err = ctr.EvalEntire(bat, ctr.nullWithBatch, i+k, proc, ctr.rewriteCond)
					if err != nil {
						return err
					}
					ctr.handleResultType(i+k, condState)
				}
			} else { // 2.2.2 : condEq in condFalse in JoinMap, further check in nullSels
				if len(ctr.nullSels) == 0 {
					ctr.handleResultType(i+k, condFalse)
					continue
				}
				condState, err = ctr.EvalEntire(bat, ctr.nullWithBatch, i+k, proc, ctr.rewriteCond)
				if err != nil {
					return err
				}
				ctr.handleResultType(i+k, condState)
			}
		}
	}
	for i, pos := range ap.Result {
		if pos >= 0 {
			ctr.rbat.Vecs[i] = bat.Vecs[pos]
			bat.Vecs[pos] = nil
		} else {
			markVec.SetNulls(ctr.markNulls)
			ctr.rbat.Vecs[i] = markVec
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	return nil
}

// store the results of the calculation on the probe side of the equation condition
func (ctr *container) evalJoinProbeCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			ctr.cleanEvalVectors()
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}

// store the results of the calculation on the build side of the equation condition
func (ctr *container) evalJoinBuildCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.buildEqEvecs {
		vec, err := ctr.buildEqEvecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			ctr.cleanEvalVectors()
			return err
		}
		ctr.buildEqVec[i] = vec
		ctr.buildEqEvecs[i].vec = vec
	}
	return nil
}

// calculate the state of non-equal conditions for those tuples in JoinMap
func (ctr *container) nonEqJoinInMap(ap *MarkJoin, mSels [][]int32, vals []uint64, k int, i int, proc *process.Process, bat *batch.Batch) (otyp, error) {
	if ap.Cond != nil {
		condState := condFalse
		if ap.HashOnPK {
			if ctr.joinBat1 == nil {
				ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
			}
			if ctr.joinBat2 == nil {
				ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.bat, proc.Mp())
			}
			if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
				1, ctr.cfs1); err != nil {
				return condUnkown, err
			}
			if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(vals[k]-1),
				1, ctr.cfs2); err != nil {
				return condUnkown, err
			}
			vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
			if err != nil {
				return condUnkown, err
			}
			if vec.GetNulls().Contains(0) {
				condState = condUnkown
			}
			bs := vector.MustFixedCol[bool](vec)
			if bs[0] {
				condState = condTrue
			}
		} else {
			sels := mSels[vals[k]-1]
			if ctr.joinBat1 == nil {
				ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
			}
			if ctr.joinBat2 == nil {
				ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.bat, proc.Mp())
			}
			for _, sel := range sels {
				if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
					1, ctr.cfs1); err != nil {
					return condUnkown, err
				}
				if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(sel),
					1, ctr.cfs2); err != nil {
					return condUnkown, err
				}
				vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
				if err != nil {
					return condUnkown, err
				}
				if vec.GetNulls().Contains(0) {
					condState = condUnkown
				}
				bs := vector.MustFixedCol[bool](vec)
				if bs[0] {
					condState = condTrue
					break
				}
			}
		}
		return condState, nil
	} else {
		return condTrue, nil
	}
}

func (ctr *container) EvalEntire(pbat, bat *batch.Batch, idx int, proc *process.Process, cond *plan.Expr) (otyp, error) {
	if cond == nil {
		return condTrue, nil
	}
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(pbat, proc.Mp())
	}
	if err := colexec.SetJoinBatchValues(ctr.joinBat, pbat, int64(idx), ctr.bat.RowCount(), ctr.cfs); err != nil {
		return condUnkown, err
	}
	vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
	if err != nil {
		return condUnkown, err
	}

	bs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
	j := uint64(vec.Length())
	hasNull := false
	for i := uint64(0); i < j; i++ {
		b, null := bs.GetValue(i)
		if null {
			hasNull = true
		} else if b {
			return condTrue, nil
		}
	}
	if hasNull {
		return condUnkown, nil
	}
	return condFalse, nil
}

// collect the idx of tuple which contains null values
func (ctr *container) evalNullSels(bat *batch.Batch) {
	jmSels := ctr.mp.Sels()
	selsMap := make(map[int32]bool)
	for _, sel := range jmSels {
		for _, i := range sel {
			selsMap[i] = true
		}
	}
	var nullSels []int64
	for i := 0; i < bat.RowCount(); i++ {
		if selsMap[int32(i)] {
			ctr.sels = append(ctr.sels, int64(i))
			continue
		}
		nullSels = append(nullSels, int64(i))
	}
	ctr.nullSels = nullSels
}

// mark probe tuple state
func (ctr *container) handleResultType(idx int, r otyp) {
	switch r {
	case condTrue:
		ctr.markVals[idx] = true
	case condFalse:
		ctr.markVals[idx] = false
	case condUnkown:
		nulls.Add(ctr.markNulls, uint64(idx))
	}
}

func DumpBatch(originBatch *batch.Batch, proc *process.Process, sels []int64) (*batch.Batch, error) {
	length := originBatch.RowCount()
	flags := make([]uint8, length)
	for _, sel := range sels {
		flags[sel] = 1
	}
	bat := batch.NewWithSize(len(originBatch.Vecs))
	for i, vec := range originBatch.Vecs {
		bat.Vecs[i] = proc.GetVector(*vec.GetType())
	}
	if len(sels) == 0 {
		return bat, nil
	}
	for i, vec := range originBatch.Vecs {
		err := bat.Vecs[i].UnionBatch(vec, 0, length, flags, proc.Mp())
		if err != nil {
			proc.PutBatch(bat)
			return nil, err
		}
	}
	bat.AddRowCount(originBatch.RowCount())
	return bat, nil
}
