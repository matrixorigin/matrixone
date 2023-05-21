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

package top

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("top([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("], %v)", ap.Limit))
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	if ap.Limit > 1024 {
		ap.ctr.sels = make([]int64, 0, 1024)
	} else {
		ap.ctr.sels = make([]int64, 0, ap.Limit)
	}
	ap.ctr.poses = make([]int32, 0, len(ap.Fs))

	ctr := ap.ctr
	ctr.executorsForOrderColumn = make([]colexec.ExpressionExecutor, len(ap.Fs))
	for i := range ctr.executorsForOrderColumn {
		ctr.executorsForOrderColumn[i], err = colexec.NewExpressionExecutor(proc, ap.Fs[i].Expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	for {
		switch ctr.state {
		case Build:
			bat := proc.InputBatch()
			if bat == nil {
				ctr.state = Eval
				continue
			}
			if bat.Length() == 0 {
				bat.Clean(proc.Mp())
				return false, nil
			}
			if ap.Limit == 0 {
				proc.PutBatch(bat)
				proc.SetInputBatch(nil)
				return true, nil
			}
			err := ctr.build(ap, bat, proc, anal)
			if err != nil {
				ap.Free(proc, true)
			}
			return false, err

		case Eval:
			if ctr.bat == nil {
				proc.SetInputBatch(nil)
				return true, nil
			}
			err := ctr.eval(ap.Limit, proc)
			ap.Free(proc, err != nil)
			return err == nil, err
		}
	}
}

func (ctr *container) build(ap *Argument, bat *batch.Batch, proc *process.Process, analyze process.Analyze) error {
	ctr.n = len(bat.Vecs)
	ctr.poses = ctr.poses[:0]
	for i := range ap.Fs {
		vec, err := ctr.executorsForOrderColumn[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return err
		}
		aNewOrderColumn := true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				aNewOrderColumn = false
				ctr.poses = append(ctr.poses, int32(j))
				break
			}
		}
		if aNewOrderColumn {
			nv, err := vec.Dup(proc.Mp())
			if err != nil {
				return err
			}
			ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
			bat.Vecs = append(bat.Vecs, nv)
			analyze.Alloc(int64(nv.Size()))
		}
	}
	if ctr.bat == nil {
		mp := make(map[int]int)
		for i, pos := range ctr.poses {
			mp[int(pos)] = i
		}
		ctr.bat = batch.NewWithSize(len(bat.Vecs))
		for i, vec := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.NewVec(*vec.GetType())
		}
		ctr.cmps = make([]compare.Compare, len(bat.Vecs))
		for i := range ctr.cmps {
			var desc, nullsLast bool
			if pos, ok := mp[i]; ok {
				desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
				if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
					nullsLast = false
				} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
					nullsLast = true
				} else {
					nullsLast = desc
				}
			}
			ctr.cmps[i] = compare.New(*bat.Vecs[i].GetType(), desc, nullsLast)
		}
	}
	defer bat.Clean(proc.Mp())
	proc.Reg.InputBatch = batch.EmptyBatch
	return ctr.processBatch(ap.Limit, bat, proc)
}

func (ctr *container) processBatch(limit int64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(len(bat.Zs))
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vec.UnionOne(bat.Vecs[j], i, proc.Mp()); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, n)
			ctr.bat.Zs = append(ctr.bat.Zs, bat.Zs[i])
			n++
		}
		if n == limit {
			ctr.sort()
		}
	}
	if start == length {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
				ctr.bat.Zs[0] = bat.Zs[i]
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *container) eval(limit int64, proc *process.Process) error {
	if int64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	sels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	if err := ctr.bat.Shuffle(sels, proc.Mp()); err != nil {
		return err
	}
	for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
	proc.Reg.InputBatch = ctr.bat
	ctr.bat = nil
	return nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}
