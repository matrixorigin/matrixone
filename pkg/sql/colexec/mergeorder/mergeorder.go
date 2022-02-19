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
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	argument := arg.(*Argument)
	buf.WriteString("mergeOrder by {")
	for i, field := range argument.Fields {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("[%s]", field.String()))
	}
	buf.WriteString("}")
}

func Prepare(_ *process.Process, arg interface{}) error {
	argument := arg.(*Argument)

	argument.ctr.state = running
	argument.ctr.attrs = make([]string, len(argument.Fields))
	argument.ctr.ds = make([]bool, len(argument.Fields))
	argument.ctr.cmps = make([]compare.Compare, len(argument.Fields))
	for i, s := range argument.Fields {
		argument.ctr.attrs[i] = s.Attr
		argument.ctr.ds[i] = s.Type == order.Descending
	}
	argument.ctr.bat = nil
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	argument := arg.(*Argument)

	if len(proc.Reg.MergeReceivers) == 1 {
		reg := proc.Reg.MergeReceivers[0]
		bat := <-reg.Ch
		if bat == nil {
			proc.Reg.MergeReceivers = nil
		}
		proc.Reg.InputBatch = bat
		return true, nil
	}

	for {
		switch argument.ctr.state {
		case running:
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
				err := mergeSort(proc.Mp, argument, bat)
				if err != nil {
					return false, err
				}
				i--
			}
			argument.ctr.state = end
		case end:
			proc.Reg.InputBatch = argument.ctr.bat
			argument.ctr.bat = nil
			return true, nil
		}
	}
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}

func mergeSort(mp *mheap.Mheap, arg *Argument, b *batch.Batch) error {
	batch.Reorder(b, arg.ctr.attrs)
	if arg.ctr.bat == nil {
		arg.ctr.bat = b
		return nil
	}
	bat1 := arg.ctr.bat
	bat2 := b
	// init structures to store result
	result := batch.New(false, arg.ctr.attrs)
	for i := 0; i < len(arg.ctr.attrs); i++ {
		result.Vecs[i] = vector.New(bat2.Vecs[i].Typ)
	}
	// init structures used to do compare work
	if arg.ctr.cmps[0] == nil {
		for k := range arg.ctr.cmps {
			arg.ctr.cmps[k] = compare.New(batch.GetVector(bat1, arg.ctr.attrs[k]).Typ.Oid, arg.ctr.ds[k])
		}
	}

	for k := range arg.ctr.cmps {
		arg.ctr.cmps[k].Set(0, batch.GetVector(bat1, arg.ctr.attrs[k]))
		arg.ctr.cmps[k].Set(1, batch.GetVector(bat2, arg.ctr.attrs[k]))
	}

	// init index-number for merge-sort
	i, j := int64(0), int64(0)
	l1, l2 := int64(vector.Length(bat1.Vecs[0])), int64(vector.Length(bat2.Vecs[0]))

	// do merge-sort work
	for i < l1 && j < l2 {
		compareResult := 0
		for k := range arg.ctr.cmps {
			compareResult = arg.ctr.cmps[k].Compare(0, 1, i, j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult >= 0 { // Weight of item1 is bigger than or equal to item2
			for k := 0; k < len(result.Vecs); k++ {
				err := vector.UnionOne(result.Vecs[k], bat1.Vecs[k], i, mp)
				if err != nil {
					return err
				}
			}
			result.Zs = append(result.Zs, bat1.Zs[i])
			i++
		} else {
			for k := 0; k < len(result.Vecs); k++ {
				err := vector.UnionOne(result.Vecs[k], bat2.Vecs[k], j, mp)
				if err != nil {
					return err
				}
			}
			result.Zs = append(result.Zs, bat2.Zs[j])
			j++
		}
	}
	if i < l1 {
		count := int(l1 - i)
		// union all bat1 from i to l1
		for k := 0; k < len(result.Vecs); k++ {
			err := vector.UnionBatch(result.Vecs[k], bat1.Vecs[k], i, count, makeFlagsOne(count), mp)
			if err != nil {
				return err
			}
		}
		result.Zs = append(result.Zs, bat1.Zs[i:]...)
	}
	if j < l2 {
		count := int(l2 - j)
		// union all bat2 from j to l2
		for k := 0; k < len(result.Vecs); k++ {
			err := vector.UnionBatch(result.Vecs[k], bat2.Vecs[k], j, count, makeFlagsOne(count), mp)
			if err != nil {
				return err
			}
		}
		result.Zs = append(result.Zs, bat1.Zs[j:]...)
	}
	return nil
}
