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

package mergetop

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	argument := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeTop %d by {", argument.Limit))
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
	argument.ctr.n = len(argument.Fields)
	argument.ctr.attrs = make([]string, len(argument.Fields))
	argument.ctr.ds = make([]bool, len(argument.Fields))
	argument.ctr.cmps = make([]compare.Compare, len(argument.Fields))
	for i, s := range argument.Fields {
		argument.ctr.attrs[i] = s.Attr
		argument.ctr.ds[i] = s.Type == top.Descending
	}
	argument.ctr.bat = nil
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	argument := arg.(*Argument)

	for {
		switch argument.ctr.state {
		case running:
			{ // do merge-top work
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
					// do init work
					if argument.ctr.bat == nil {
						argument.ctr.bat = batch.New(true, bat.Attrs)
						for k, vec := range bat.Vecs {
							argument.ctr.bat.Vecs[k] = vector.New(vec.Typ)
						}

						for k := range argument.ctr.cmps {
							argument.ctr.cmps[k] = compare.New(batch.GetVector(bat, argument.ctr.attrs[k]).Typ.Oid, argument.ctr.ds[k])
						}
					}

					err := argument.ctr.mergeTop(proc, argument.Limit, bat)
					if err != nil {
						return false, err
					}
					i--
				}
			}
			{ // pop final result
				ctr := &argument.ctr
				if ctr.bat != nil {
					if int64(len(ctr.sels)) < argument.Limit {
						ctr.sort()
					}

					for i, cmp := range ctr.cmps {
						ctr.bat.Vecs[i] = cmp.Vector()
					}

					data, err := mheap.Alloc(proc.Mp, int64(len(ctr.sels))*8)
					if err != nil {
						return false, err
					}
					sels := encoding.DecodeInt64Slice(data)
					for i, j := 0, len(ctr.sels); i < j; i++ {
						sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
					}
					ctr.bat.Sels = sels
					ctr.bat.SelsData = data

					err = batch.Shuffle(ctr.bat, proc.Mp)
					if err != nil {
						return false, err
					}
				}
			}
			argument.ctr.state = end
		case end:
			proc.Reg.InputBatch = argument.ctr.bat
			argument.ctr.bat = nil
			argument.ctr.sels = nil
			return true, nil
		}
	}
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}

func (ctr *container) mergeTop(proc *process.Process, limit int64, b *batch.Batch) error {
	if ctr.sels == nil {
		data, err := mheap.Alloc(proc.Mp, limit*8)
		if err != nil {
			return err
		}
		ctr.sels = encoding.DecodeInt64Slice(data)
		ctr.sels = ctr.sels[:0]
	}

	var start int64
	bLength := int64(vector.Length(b.Vecs[0]))
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > bLength {
			start = bLength
		}

		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionOne(vec, b.Vecs[j], i, proc.Mp); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, n)
			ctr.bat.Zs = append(ctr.bat.Zs, b.Zs[i])
			n++
		}

		if n == limit {
			ctr.sort()
		}
	}
	if start == bLength {
		return nil
	}

	// b is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, b.Vecs[i])
	}
	for i, j := start, bLength; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
				ctr.bat.Zs[0] = b.Zs[i]
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}
