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
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/partition"
	"matrixone/pkg/sort"
	"matrixone/pkg/vm/process"
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
	ctr := &n.Ctr
	{
		ctr.ds = make([]bool, len(n.Fs))
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
			ctr.ds[i] = f.Type == Descending
		}
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.InputBatch == nil {
		return false, nil
	}
	bat := proc.Reg.InputBatch.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	ctr := &n.Ctr
	if len(bat.Sels) > 0 {
		bat.Shuffle(proc)
	}
	if err := ctr.processBatch(bat, proc); err != nil {
		bat.Clean(proc)
		return false, err
	}
	proc.Reg.InputBatch = bat
	return false, nil
}

func (ctr *Container) processBatch(bat *batch.Batch, proc *process.Process) error {
	ovec := bat.GetVector(ctr.attrs[0])
	n := ovec.Length()
	data, err := proc.Alloc(int64(n * 8))
	if err != nil {
		return err
	}
	sels := encoding.DecodeInt64Slice(data)
	{
		for i := range sels {
			sels[i] = int64(i)
		}
	}
	sort.Sort(ctr.ds[0], sels, ovec)
	if len(ctr.attrs) == 1 {
		bat.Sels = sels
		bat.SelsData = data
		return nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.attrs); i < j; i++ {
		desc := ctr.ds[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := bat.GetVector(ctr.attrs[i])
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(desc, sels[ps[i]:], vec)
			} else {
				sort.Sort(desc, sels[ps[i]:ps[i+1]], vec)
			}
		}
		ovec = vec
	}
	bat.Sels = sels
	bat.SelsData = data
	return nil
}
