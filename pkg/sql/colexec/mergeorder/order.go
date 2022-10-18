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

package mergeorder

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("mergeorder([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.poses = make([]int32, 0, len(ap.Fs))
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
					vector.Clean(ctr.bat.Vecs[i], proc.Mp())
				}
				ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
				ctr.bat.ExpandNulls()
			}
			anal.Output(ctr.bat)
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			ctr.state = End
			return true, nil
		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}

}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	for {
		if len(proc.Reg.MergeReceivers) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
			reg := proc.Reg.MergeReceivers[i]
			bat := <-reg.Ch
			if bat == nil {
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i--
				continue
			}
			if bat.Length() == 0 {
				i--
				continue
			}
			bat.ExpandNulls()
			anal.Input(bat)
			anal.Alloc(int64(bat.Size()))
			ctr.n = len(bat.Vecs)
			ctr.poses = ctr.poses[:0]
			for _, f := range ap.Fs {
				vec, err := colexec.EvalExpr(bat, proc, f.Expr)
				if err != nil {
					return err
				}
				flg := true
				for i := range bat.Vecs {
					if bat.Vecs[i] == vec {
						flg = false
						ctr.poses = append(ctr.poses, int32(i))
						break
					}
				}
				if flg {
					ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
					bat.Vecs = append(bat.Vecs, vec)
				}
			}
			if ctr.bat == nil {
				mp := make(map[int]int)
				for i, pos := range ctr.poses {
					mp[int(pos)] = i
				}
				ctr.bat = bat
				ctr.cmps = make([]compare.Compare, len(bat.Vecs))
				var desc, nullsLast bool
				for i := range ctr.cmps {
					if pos, ok := mp[i]; ok {
						desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
						if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
							nullsLast = false
						} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
							nullsLast = true
						} else {
							nullsLast = desc
						}
						ctr.cmps[i] = compare.New(bat.Vecs[i].Typ, desc, nullsLast)
					}
				}
			} else {
				if err := ctr.processBatch(bat, proc); err != nil {
					bat.Clean(proc.Mp())
					ctr.bat.Clean(proc.Mp())
					return err
				}
				bat.Clean(proc.Mp())
			}
		}
	}
	return nil
}

func (ctr *container) processBatch(bat2 *batch.Batch, proc *process.Process) error {
	bat1 := ctr.bat
	rbat := batch.NewWithSize(len(bat1.Vecs))
	for i, vec := range bat1.Vecs {
		rbat.Vecs[i] = vector.New(vec.Typ)
	}
	for i, cmp := range ctr.cmps {
		if cmp != nil {
			cmp.Set(0, bat1.GetVector(int32(i)))
			cmp.Set(1, bat2.GetVector(int32(i)))
		}
	}
	// init index-number for merge-sort
	i, j := int64(0), int64(0)
	l1, l2 := int64(vector.Length(bat1.Vecs[0])), int64(vector.Length(bat2.Vecs[0]))

	// do merge-sort work
	for i < l1 && j < l2 {
		compareResult := 0
		for _, pos := range ctr.poses {
			compareResult = ctr.cmps[pos].Compare(0, 1, i, j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult <= 0 { // Weight of item1 is less than or equal to item2
			for k := 0; k < len(rbat.Vecs); k++ {
				err := vector.UnionOne(rbat.Vecs[k], bat1.Vecs[k], i, proc.Mp())
				if err != nil {
					rbat.Clean(proc.Mp())
					return err
				}
			}
			rbat.Zs = append(rbat.Zs, bat1.Zs[i])
			i++
		} else {
			for k := 0; k < len(rbat.Vecs); k++ {
				err := vector.UnionOne(rbat.Vecs[k], bat2.Vecs[k], j, proc.Mp())
				if err != nil {
					rbat.Clean(proc.Mp())
					return err
				}
			}
			rbat.Zs = append(rbat.Zs, bat2.Zs[j])
			j++
		}
	}
	if i < l1 {
		count := int(l1 - i)
		// union all bat1 from i to l1
		for k := 0; k < len(rbat.Vecs); k++ {
			err := vector.UnionBatch(rbat.Vecs[k], bat1.Vecs[k], i, count, makeFlagsOne(count), proc.Mp())
			if err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		}
		rbat.Zs = append(rbat.Zs, bat1.Zs[i:]...)
	}
	if j < l2 {
		count := int(l2 - j)
		// union all bat2 from j to l2
		for k := 0; k < len(rbat.Vecs); k++ {
			err := vector.UnionBatch(rbat.Vecs[k], bat2.Vecs[k], j, count, makeFlagsOne(count), proc.Mp())
			if err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		}
		rbat.Zs = append(rbat.Zs, bat2.Zs[j:]...)
	}
	ctr.bat.Clean(proc.Mp())
	ctr.bat = rbat
	return nil
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}
