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
	"fmt"

	compare "github.com/matrixorigin/matrixone/pkg/compare2"
	batch "github.com/matrixorigin/matrixone/pkg/container/batch2"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	order "github.com/matrixorigin/matrixone/pkg/sql/colexec2/order"
	process "github.com/matrixorigin/matrixone/pkg/vm/process2"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("])"))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	{
		n.ctr.poses = make([]int32, len(n.Fs))
		for i, f := range n.Fs {
			n.ctr.poses[i] = f.Pos
		}
	}
	n.ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := n.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
			ctr.state = End
			return true, nil
		default:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}

}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
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
			if len(bat.Zs) == 0 {
				i--
				continue
			}
			if ctr.bat == nil {
				batch.Reorder(bat, ctr.poses)
				ctr.bat = bat
				for i, f := range n.Fs {
					ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == order.Descending)
				}
				for i := len(n.Fs); i < len(bat.Vecs); i++ {
					ctr.cmps = append(ctr.cmps, compare.New(bat.Vecs[i].Typ.Oid, true))
				}
			} else {
				batch.Reorder(bat, ctr.poses)
				if err := ctr.processBatch(bat, proc); err != nil {
					batch.Clean(bat, proc.Mp)
					batch.Clean(ctr.bat, proc.Mp)
					return err
				}
				batch.Clean(bat, proc.Mp)
			}
		}
	}
	return nil
}

func (ctr *Container) processBatch(bat2 *batch.Batch, proc *process.Process) error {
	bat1 := ctr.bat
	rbat := batch.New(len(bat1.Vecs))
	for i, vec := range bat1.Vecs {
		rbat.Vecs[i] = vector.New(vec.Typ)
	}
	for i, cmp := range ctr.cmps {
		cmp.Set(0, batch.GetVector(bat1, int32(i)))
		cmp.Set(1, batch.GetVector(bat2, int32(i)))
	}
	// init index-number for merge-sort
	i, j := int64(0), int64(0)
	l1, l2 := int64(vector.Length(bat1.Vecs[0])), int64(vector.Length(bat2.Vecs[0]))

	// do merge-sort work
	for i < l1 && j < l2 {
		compareResult := 0
		for k := range ctr.cmps {
			compareResult = ctr.cmps[k].Compare(0, 1, i, j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult <= 0 { // Weight of item1 is less than or equal to item2
			for k := 0; k < len(rbat.Vecs); k++ {
				err := vector.UnionOne(rbat.Vecs[k], bat1.Vecs[k], i, proc.Mp)
				if err != nil {
					batch.Clean(rbat, proc.Mp)
					return err
				}
			}
			rbat.Zs = append(rbat.Zs, bat1.Zs[i])
			i++
		} else {
			for k := 0; k < len(rbat.Vecs); k++ {
				err := vector.UnionOne(rbat.Vecs[k], bat2.Vecs[k], j, proc.Mp)
				if err != nil {
					batch.Clean(rbat, proc.Mp)
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
			err := vector.UnionBatch(rbat.Vecs[k], bat1.Vecs[k], i, count, makeFlagsOne(count), proc.Mp)
			if err != nil {
				batch.Clean(rbat, proc.Mp)
				return err
			}
		}
		rbat.Zs = append(rbat.Zs, bat1.Zs[i:]...)
	}
	if j < l2 {
		count := int(l2 - j)
		// union all bat2 from j to l2
		for k := 0; k < len(rbat.Vecs); k++ {
			err := vector.UnionBatch(rbat.Vecs[k], bat2.Vecs[k], j, count, makeFlagsOne(count), proc.Mp)
			if err != nil {
				batch.Clean(rbat, proc.Mp)
				return err
			}
		}
		rbat.Zs = append(rbat.Zs, bat2.Zs[j:]...)
	}
	batch.Clean(ctr.bat, proc.Mp)
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
