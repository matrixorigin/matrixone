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
	"unsafe"

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
	return nil
}

// New Logic: frist the right table is used to build a hashtable.
// different from before, the hashtable has been build before the
// Call func. And note that, the all batches from the right has been
// used to build it and not only one batch.
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
				ctr.state = End
				if ctr.mp != nil {
					ctr.mp.Free()
				}
				return true, err
			}
			ctr.state = Probe
		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				if ctr.mp != nil {
					ctr.mp.Free()
				}
				if ctr.bat != nil {
					ctr.bat.Clean(proc.GetMheap())
				}
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				if err := ctr.emptyProbe(bat, ap, proc, anal); err != nil {
					ctr.state = End
					if ctr.mp != nil {
						ctr.mp.Free()
					}
					proc.SetInputBatch(nil)
					return true, err
				}
			} else {
				if err := ctr.probe(bat, ap, proc, anal); err != nil {
					ctr.state = End
					if ctr.mp != nil {
						ctr.mp.Free()
					}
					proc.SetInputBatch(nil)
					return true, err
				}
			}
			return false, nil
		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.Ht.(*hashmap.JoinMap).Dup()
		ctr.hasNull = ctr.mp.HasNull()
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	defer bat.Clean(proc.GetMheap())
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result) + 1)
	rbat.Zs = proc.GetMheap().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(bat.Vecs[pos].Typ)
	}
	rbat.Vecs[len(ap.Result)] = vector.New(types.T_bool.ToType())
	ctr.joinFlags = make([]bool, bat.Length())
	if ap.OutputMark {
		ctr.Nsp = nulls.NewWithSize(bat.Length())
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.joinFlags[0])), cap(ctr.joinFlags))[:len(ctr.joinFlags)]
		// add mark flag, the initial
		rbat.Vecs[len(ap.Result)] = vector.NewWithData(types.T_bool.ToType(), data, ctr.joinFlags, ctr.Nsp)
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
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[pos], int64(i+k), proc.GetMheap()); err != nil {
						rbat.Clean(proc.GetMheap())
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
	defer bat.Clean(proc.Mp)
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result) + 1)
	rbat.Zs = proc.GetMheap().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(bat.Vecs[pos].Typ)
	}
	lastIndex := len(rbat.Vecs) - 1
	rbat.Vecs[lastIndex] = vector.New(types.T_bool.ToType())
	ctr.joinFlags = make([]bool, bat.Length())
	ctr.Nsp = nulls.NewWithSize(bat.Length())
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc); err != nil {
		return err
	}
	defer ctr.freeJoinCondition(proc)
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
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 {
				continue
			}
			if zvals[k] == 0 {
				ctr.Nsp.Np.Add(uint64(i + k))
			} else if vals[k] > 0 {
				ctr.joinFlags[i+k] = true
				flag, err := ctr.NoneEqJoin(ap, mSels, vals, k, i, proc, bat, rbat)
				if err != nil {
					rbat.Clean(proc.GetMheap())
					if ctr.mp != nil {
						ctr.mp.Free()
					}
					if ctr.bat != nil {
						ctr.bat.Clean(proc.GetMheap())
					}
					return err
				}
				ctr.joinFlags[i+k] = ctr.joinFlags[i+k] && flag
			} else if ctr.hasNull {
				ctr.Nsp.Np.Add(uint64(i + k))
			}
		}
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.joinFlags[0])), cap(ctr.joinFlags))[:len(ctr.joinFlags)]
		// add mark flag, the initial
		rbat.Vecs[len(ap.Result)] = vector.NewWithData(types.T_bool.ToType(), data, ctr.joinFlags, ctr.Nsp)
		markVec := vector.NewWithData(types.T_bool.ToType(), data, ctr.joinFlags, ctr.Nsp)
		for k := 0; k < n; k++ {
			if ctr.Nsp.Np.Contains(uint64(i+k)) && ap.OutputNull || !ctr.Nsp.Np.Contains(uint64(i+k)) && ctr.joinFlags[i+k] == ap.MarkMeaning {
				for j, pos := range ap.Result {
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[pos], int64(i+k), proc.GetMheap()); err != nil {
						rbat.Clean(proc.GetMheap())
						return err
					}
				}
				if ap.OutputMark {
					if err := vector.UnionOne(rbat.Vecs[lastIndex], markVec, int64(i+k), proc.GetMheap()); err != nil {
						rbat.Clean(proc.GetMheap())
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

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(proc.GetMheap()) == nil {
			for j := 0; j < i; j++ {
				if ctr.evecs[j].needFree {
					vector.Clean(ctr.evecs[j].vec, proc.GetMheap())
				}
			}
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

func (ctr *container) freeJoinCondition(proc *process.Process) {
	for i := range ctr.evecs {
		if ctr.evecs[i].needFree {
			ctr.evecs[i].vec.Free(proc.GetMheap())
		}
	}
}

// func (ctr *container) freeSels(proc *process.Process) {
// 	for i := range ctr.sels {
// 		proc.GetMheap().PutSels(ctr.sels[i])
// 	}
// 	ctr.sels = nil
// }

// return true means Joined successfully, false means Joining falied
func (ctr *container) NoneEqJoin(ap *Argument, mSels [][]int64, vals []uint64, k int, i int, proc *process.Process, bat *batch.Batch, rbat *batch.Batch) (bool, error) {
	if ap.Cond != nil {
		flg := true // mark no tuple satisfies the condition
		sels := mSels[vals[k]-1]
		for _, sel := range sels {
			vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
			if err != nil {
				return false, err
			}
			bs := vec.Col.([]bool)
			if bs[0] {
				flg = false
				vec.Free(proc.Mp)
				break
			}
			vec.Free(proc.Mp)
		}
		if !flg {
			return true, nil
		} else {
			return false, nil
		}
	} else {
		return true, nil
	}
}
