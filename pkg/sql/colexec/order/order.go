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

package order

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	{
		ap.ctr.desc = make([]bool, len(ap.Fs))
		ap.ctr.nullsLast = make([]bool, len(ap.Fs))
		ap.ctr.vecs = make([]evalVector, len(ap.Fs))
		for i, f := range ap.Fs {
			ap.ctr.desc[i] = f.Flag&plan.OrderBySpec_DESC != 0
			if f.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
				ap.ctr.nullsLast[i] = false
			} else if f.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
				ap.ctr.nullsLast[i] = true
			} else {
				ap.ctr.nullsLast[i] = ap.ctr.desc[i]
			}
		}
	}

	ctr := ap.ctr
	ctr.vecs = make([]evalVector, len(ap.Fs))
	for i := range ctr.vecs {
		ctr.vecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Fs[i].Expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	bat := proc.InputBatch()
	ap := arg.(*Argument)
	if bat == nil {
		ap.Free(proc, false)
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}
	end, err := ap.ctr.process(bat, proc)
	if err != nil {
		ap.Free(proc, true)
		return false, err
	}
	return end, nil
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) (bool, error) {
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		if vec.NeedDup() {
			oldVec := bat.Vecs[i]
			nvec, err := oldVec.Dup(proc.Mp())
			if err != nil {
				return false, err
			}
			bat.ReplaceVector(oldVec, nvec)
			oldVec.Free(proc.Mp())
		}
	}

	for i := range ctr.vecs {
		vec, err := ctr.vecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return false, err
		}
		ctr.vecs[i].vec = vec
	}

	ovec := ctr.vecs[0].vec
	var strCol []string

	sels := make([]int64, len(bat.Zs))
	for i := 0; i < len(bat.Zs); i++ {
		sels[i] = int64(i)
	}

	// skip sort for const vector
	if !ovec.IsConst() {
		nullCnt := ovec.GetNulls().Count()
		if nullCnt < ovec.Length() {
			if ovec.GetType().IsVarlen() {
				strCol = vector.MustStrCol(ovec)
			} else {
				strCol = nil
			}
			sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, sels, ovec, strCol)
		}
	}
	if len(ctr.vecs) == 1 {
		if err := bat.Shuffle(sels, proc.Mp()); err != nil {
			panic(err)
		}
		return false, nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.vecs); i < j; i++ {
		desc := ctr.desc[i]
		nullsLast := ctr.nullsLast[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := ctr.vecs[i].vec
		// skip sort for const vector
		if !vec.IsConst() {
			nullCnt := vec.GetNulls().Count()
			if nullCnt < vec.Length() {
				if vec.GetType().IsVarlen() {
					strCol = vector.MustStrCol(vec)
				} else {
					strCol = nil
				}
				for i, j := 0, len(ps); i < j; i++ {
					if i == j-1 {
						sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[i]:], vec, strCol)
					} else {
						sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[i]:ps[i+1]], vec, strCol)
					}
				}
			}
		}
		ovec = vec
	}
	if err := bat.Shuffle(sels, proc.Mp()); err != nil {
		panic(err)
	}
	return false, nil
}
