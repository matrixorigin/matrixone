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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" mark join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.buildEqVec = make([]*vector.Vector, len(ap.Conditions[1]))
	ap.ctr.buildEqEvecs = make([]evalVector, len(ap.Conditions[1]))
	return nil
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

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				return false, err
			}
			ctr.state = Probe

		case Probe:
			var err error
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				err = ctr.emptyProbe(bat, ap, proc, anal)
			} else {
				err = ctr.probe(bat, ap, proc, anal)
			}
			bat.Clean(proc.Mp())
			return false, err

		default:
			ap.Free(proc, false)
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	var err error
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		joinMap := bat.Ht.(*hashmap.JoinMap)
		ctr.evalNullSels(bat)
		ctr.nullWithBatch, err = DumpBatch(bat, proc, ctr.nullSels)
		if err != nil {
			return err
		}
		if err = ctr.evalJoinBuildCondition(bat, ap.Conditions[1], proc); err != nil {
			return err
		}
		ctr.rewriteCond = colexec.RewriteFilterExprList(ap.OnList)
		ctr.bat = bat
		ctr.mp = joinMap.Dup()
		ctr.hasNull = ctr.mp.HasNull()
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result) + 1)
	rbat.Zs = proc.Mp().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(bat.Vecs[pos].Typ)
	}
	rbat.Vecs[len(ap.Result)] = vector.New(types.T_bool.ToType())
	ctr.joinFlags = make([]bool, bat.Length())
	if ap.OutputMark {
		ctr.Nsp = nulls.NewWithSize(bat.Length())
		// add mark flag, the initial
		rbat.Vecs[len(ap.Result)] = vector.NewWithFixed(types.T_bool.ToType(), ctr.joinFlags, ctr.Nsp, proc.Mp())
	}
	count := bat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for k := 0; k < n; k++ {
			if ap.MarkMeaning == ctr.joinFlags[i+k] {
				for j, pos := range ap.Result {
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[pos], int64(i+k), proc.Mp()); err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
				rbat.Zs = append(rbat.Zs, bat.Zs[i+k])
			}
		}
	}
	if !ap.OutputMark {
		rbat.Vecs = rbat.Vecs[:len(rbat.Vecs)-1]
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result) + 1)
	// vector.UnionBatch()
	rbat.Zs = proc.Mp().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(bat.Vecs[pos].Typ)
	}
	lastIndex := len(rbat.Vecs) - 1
	rbat.Vecs[lastIndex] = vector.New(types.T_bool.ToType())
	ctr.joinFlags = make([]bool, bat.Length())
	ctr.Nsp = nulls.NewWithSize(bat.Length())
	ctr.cleanEvalVectors(proc.Mp())
	if err := ctr.evalJoinProbeCondition(bat, ap.Conditions[0], proc); err != nil {
		return err
	}
	count := bat.Length()
	itr := ctr.mp.Map().NewIterator()
	mSels := ctr.mp.Sels()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		var condState resultType
		// var condNonEq resultType
		// var condEq resultType
		var err error
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 {
				continue
			}
			if zvals[k] == 0 { // 2.1 : probe tuple has null
				condState, err = ctr.EvalEntire(bat, ctr.bat, i+k, proc, ctr.rewriteCond)
				if err != nil {
					rbat.Clean(proc.Mp())
					return err
				}
				ctr.handleResultType(i+k, condState)
			} else if vals[k] > 0 { // 2.2.1 : condEq is condTrue in JoinMap
				condState, err = ctr.nonEqJoinInMap(ap, mSels, vals, k, i, proc, bat)
				if err != nil {
					rbat.Clean(proc.Mp())
					return err
				}
				if condState == condTrue { // 2.2.1.1 : condNonEq is condTrue in JoinMap
					ctr.joinFlags[i+k] = true
				} else if condState == condUnkown { // 2.2.1.2 : condNonEq is condUnkown in JoinMap
					ctr.Nsp.Np.Add(uint64(i + k))
				} else { // 2.2.1.3 : condNonEq is condFalse in JoinMap, further check in nullSels
					if len(ctr.nullSels) == 0 {
						ctr.handleResultType(i+k, condFalse)
						continue
					}
					condState, err = ctr.EvalEntire(bat, ctr.nullWithBatch, i+k, proc, ctr.rewriteCond)
					if err != nil {
						rbat.Clean(proc.Mp())
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
					rbat.Clean(proc.Mp())
					return err
				}
				ctr.handleResultType(i+k, condState)
			}
		}
		// add mark flag, the initial
		rbat.Vecs[len(ap.Result)] = vector.NewWithFixed(types.T_bool.ToType(), ctr.joinFlags, ctr.Nsp, proc.Mp())
		markVec := vector.NewWithFixed(types.T_bool.ToType(), ctr.joinFlags, ctr.Nsp, proc.Mp())
		for k := 0; k < n; k++ {
			if ap.OutputAnyway || (ctr.Nsp.Np.Contains(uint64(i+k)) && ap.OutputNull || !ctr.Nsp.Np.Contains(uint64(i+k)) && ctr.joinFlags[i+k] == ap.MarkMeaning) {
				for j, pos := range ap.Result {
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[pos], int64(i+k), proc.Mp()); err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
				if ap.OutputMark {
					if err := vector.UnionOne(rbat.Vecs[lastIndex], markVec, int64(i+k), proc.Mp()); err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
				rbat.Zs = append(rbat.Zs, bat.Zs[i+k])
			}
		}
	}
	if !ap.OutputMark {
		rbat.Vecs = rbat.Vecs[:len(rbat.Vecs)-1]
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}

// store the results of the calculation on the probe side of the equation condition
func (ctr *container) evalJoinProbeCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(proc.Mp()) == nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
		ctr.evecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.evecs[i].needFree = false
				break
			}
		}
	}
	return nil
}

// store the results of the calculation on the build side of the equation condition
func (ctr *container) evalJoinBuildCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(proc.Mp()) == nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.buildEqVec[i] = vec
		ctr.buildEqEvecs[i].vec = vec
		ctr.buildEqEvecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.buildEqEvecs[i].needFree = false
				break
			}
		}
	}
	return nil
}

// calculate the state of non-equal conditions for those tuples in JoinMap
func (ctr *container) nonEqJoinInMap(ap *Argument, mSels [][]int64, vals []uint64, k int, i int, proc *process.Process, bat *batch.Batch) (resultType, error) {
	if ap.Cond != nil {
		condState := condFalse
		sels := mSels[vals[k]-1]
		for _, sel := range sels {
			vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
			if err != nil {
				return condUnkown, err
			}
			if vec.Nsp.Contains(0) {
				condState = condUnkown
			}
			bs := vec.Col.([]bool)
			if bs[0] {
				condState = condTrue
				vec.Free(proc.Mp())
				break
			}
			vec.Free(proc.Mp())
		}
		return condState, nil
	} else {
		return condTrue, nil
	}
}

func (ctr *container) EvalEntire(pbat, bat *batch.Batch, idx int, proc *process.Process, cond *plan.Expr) (resultType, error) {
	if cond == nil {
		return condTrue, nil
	}
	vec, err := colexec.JoinFilterEvalExpr(pbat, bat, idx, proc, cond)
	defer vec.Free(proc.Mp())
	if err != nil {
		return condUnkown, err
	}
	bs := vec.Col.([]bool)
	for _, b := range bs {
		if b {
			return condTrue, nil
		}
	}
	if nulls.Any(vec.Nsp) {
		return condUnkown, nil
	}
	return condFalse, nil
}

// collect the idx of tuple which contains null values
func (ctr *container) evalNullSels(bat *batch.Batch) {
	joinMap := bat.Ht.(*hashmap.JoinMap)
	jmSels := joinMap.Sels()
	selsMap := make(map[int64]bool)
	for _, sel := range jmSels {
		for _, i := range sel {
			selsMap[i] = true
		}
	}
	var nullSels []int64
	for i := 0; i < bat.Length(); i++ {
		if selsMap[int64(i)] {
			ctr.sels = append(ctr.sels, int64(i))
			continue
		}
		nullSels = append(nullSels, int64(i))
	}
	ctr.nullSels = nullSels
}

// mark probe tuple state
func (ctr *container) handleResultType(idx int, r resultType) {
	switch r {
	case condTrue:
		ctr.joinFlags[idx] = true
	case condFalse:
		ctr.joinFlags[idx] = false
	case condUnkown:
		ctr.Nsp.Np.Add(uint64(idx))
	}
}

func DumpBatch(originBatch *batch.Batch, proc *process.Process, sels []int64) (*batch.Batch, error) {
	length := originBatch.Length()
	flags := make([]uint8, length)
	for _, sel := range sels {
		flags[sel] = 1
	}
	bat := batch.NewWithSize(len(originBatch.Vecs))
	for i, vec := range originBatch.Vecs {
		bat.Vecs[i] = vector.New(vec.GetType())
	}
	if len(sels) == 0 {
		return bat, nil
	}
	for i, vec := range originBatch.Vecs {
		err := vector.UnionBatch(bat.Vecs[i], vec, 0, length, flags, proc.Mp())
		if err != nil {
			return nil, err
		}
	}

	bat.Zs = append(bat.Zs, originBatch.Zs...)
	return bat, nil
}
