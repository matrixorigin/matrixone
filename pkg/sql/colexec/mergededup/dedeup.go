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

package mergededup

import (
	"bytes"
	"errors"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/vm/process"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("δ(%v)", n.Attrs))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		n:      len(n.Attrs),
		slots:  fastmap.New(),
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		groups: make(map[uint64][]*hash.SetGroup),
		vec:    vector.New(types.Type{Oid: types.T_int8}),
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
			if !n.Flg && ctr.bat != nil {
				ctr.bat.Reduce(n.Attrs, proc)
			}
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			ctr.clean(proc)
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.Ax = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(n.Attrs)
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
			if err := ctr.buildBatch(bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			reg.Wg.Done()
			bat.Clean(proc)
		}
	}
	return nil
}

func (ctr *Container) buildBatch(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitDedup(i, length, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitDedup(start int, count int, vecs []*vector.Vector, proc *process.Process) error {
	var err error

	copy(ctr.hashs[:count], OneUint64s[:count])
	ctr.fillHash(start, count, vecs[:ctr.n])
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for _, g := range gs {
					remaining = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs, proc)
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.SetGroup, 0, 8)
			}
			for len(remaining) > 0 {

				g := hash.NewSetGroup(ctr.rows)
				{
					for i, vec := range ctr.bat.Vecs {
						if err = vec.UnionOne(vecs[i], remaining[0], proc); err != nil {
							return err
						}
					}
				}
				ctr.rows++
				ctr.groups[h] = append(ctr.groups[h], g)
				remaining = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs, proc)
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				if proc.Size() > proc.Lim.Size {
					return errors.New("out of memory")
				}
			}
			ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
		}
	}
	ctr.slots.Reset()
	return nil
}

func (ctr *Container) fillHash(start, count int, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, ctr.hashs, vec.Window(start, start+count, ctr.vec))
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots.Get(h)
		if !ok {
			slot = nextslot
			ctr.slots.Set(h, slot)
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], int64(i+start))
	}
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
		ctr.bat = nil
	}
	{
		for _, reg := range proc.Reg.Ws {
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
