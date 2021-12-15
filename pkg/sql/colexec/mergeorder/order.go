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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
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
	buf.WriteString(fmt.Sprintf("])"))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	ctr.ds = make([]bool, len(n.Fs))
	ctr.attrs = make([]string, len(n.Fs))
	for i, f := range n.Fs {
		ctr.attrs[i] = f.Attr
		ctr.ds[i] = f.Type == Descending
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat == nil {
				proc.Reg.InputBatch = nil
				ctr.state = End
				return true, nil
			}
			if err := ctr.eval(proc); err != nil {
				ctr.state = End
				return true, err
			}
			if !n.Flg {
				ctr.bat.Reduce(ctr.attrs, proc)
			}
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
			ctr.clean(proc)
			ctr.state = End
			return true, nil
		case End:
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
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(ctr.attrs)
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
			} else {
				bat.Reorder(ctr.bat.Attrs)
			}
			if len(bat.Sels) > 0 {
				bat.Shuffle(proc)
			}
			for sel, nsel := int64(0), int64(bat.Vecs[0].Length()); sel < nsel; sel++ {
				for i, vec := range ctr.bat.Vecs {
					if err := vec.UnionOne(bat.Vecs[i], sel, proc); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						bat.Clean(proc)
						return err
					}
				}
			}
			if proc.Size() > proc.Lim.Size {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return errors.New("out of memory")
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) eval(proc *process.Process) error {
	ovec := ctr.bat.Vecs[0]
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
		ctr.bat.Sels = sels
		ctr.bat.SelsData = data
		return nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.attrs); i < j; i++ {
		desc := ctr.ds[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := ctr.bat.Vecs[i]
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(desc, sels[ps[i]:], vec)
			} else {
				sort.Sort(desc, sels[ps[i]:ps[i+1]], vec)
			}
		}
		ovec = vec
	}
	ctr.bat.Sels = sels
	ctr.bat.SelsData = data
	return nil
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	{
		for _, reg := range proc.Reg.MergeReceivers {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
