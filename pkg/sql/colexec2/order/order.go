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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	{
		n.ctr.ds = make([]bool, len(n.Fs))
		n.ctr.poses = make([]int32, len(n.Fs))
		for i, f := range n.Fs {
			n.ctr.poses[i] = f.Pos
			n.ctr.ds[i] = f.Type == Descending
		}
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	return n.ctr.process(bat, proc)
}

func (ctr *Container) process(bat *batch.Batch, proc *process.Process) (bool, error) {
	ovec := bat.GetVector(ctr.poses[0])
	n := len(bat.Zs)
	sels := make([]int64, n)
	for i := range sels {
		sels[i] = int64(i)
	}
	sort.Sort(ctr.ds[0], sels, ovec)
	if len(ctr.poses) == 1 {
		if err := bat.Shuffle(sels, proc.Mp); err != nil {
			panic(err)
		}
		return false, nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.poses); i < j; i++ {
		desc := ctr.ds[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := bat.GetVector(ctr.poses[i])
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(desc, sels[ps[i]:], vec)
			} else {
				sort.Sort(desc, sels[ps[i]:ps[i+1]], vec)
			}
		}
		ovec = vec
	}
	if err := bat.Shuffle(sels, proc.Mp); err != nil {
		panic(err)
	}
	return false, nil
}
