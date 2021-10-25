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

package inner

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/hash"
	"github.com/matrixorigin/matrixone/pkg/intmap/fastmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	buf.WriteString(fmt.Sprintf("%s ⨝ %s", n.R, n.S))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		n:      len(n.Rattrs),
		slots:  fastmap.New(),
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		groups: make(map[uint64][]*hash.BagGroup),
		vec:    vector.New(types.Type{Oid: types.T_int8}),
	}
	{
		for _, attr := range n.Rattrs {
			n.Ctr.rattrs = append(n.Ctr.rattrs, n.R+"."+attr)
		}
		for _, attr := range n.Sattrs {
			n.Ctr.rattrs = append(n.Ctr.rattrs, n.S+"."+attr)
		}
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n.Sattrs, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			ok, err := ctr.probe(n.R, n.S, n.Rattrs, proc)
			if err != nil || ok {
				ctr.state = End
				ctr.clean(proc)
				return ok, err
			}
			return ok, err
		case End:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

// R ⨝ S - S is the smaller relation
func (ctr *Container) build(attrs []string, proc *process.Process) error {
	var err error

	reg := proc.Reg.MergeReceivers[1]
	for {
		v := <-reg.Ch
		if v == nil {
			reg.Ch = nil
			reg.Wg.Done()
			break
		}
		bat := v.(*batch.Batch)
		if bat == nil || bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		if ctr.bat == nil {
			bat.Reorder(attrs)
			ctr.bat = batch.New(true, bat.Attrs)
			for i, attr := range bat.Attrs {
				vec := bat.GetVector(attr)
				ctr.bat.Vecs[i] = vector.New(vec.Typ)
			}
		} else {
			bat.Reorder(ctr.bat.Attrs)
		}
		if err = ctr.buildBatch(bat.Vecs, proc); err != nil {
			reg.Ch = nil
			reg.Wg.Done()
			bat.Clean(proc)
			return err
		}
		reg.Wg.Done()
		bat.Clean(proc)
	}
	return nil
}

func (ctr *Container) probe(rName, sName string, attrs []string, proc *process.Process) (bool, error) {
	reg := proc.Reg.MergeReceivers[0]
	for {
		v := <-reg.Ch
		if v == nil {
			reg.Ch = nil
			reg.Wg.Done()
			proc.Reg.InputBatch = nil
			ctr.clean(proc)
			return true, nil
		}
		bat := v.(*batch.Batch)
		if bat == nil || bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		if len(ctr.groups) == 0 {
			reg.Ch = nil
			reg.Wg.Done()
			proc.Reg.InputBatch = nil
			bat.Clean(proc)
			return true, nil
		}
		if len(ctr.Probe.attrs) == 0 {
			bat.Reorder(attrs)
			ctr.Probe.attrs = append(ctr.Probe.attrs, bat.Attrs...)
			ctr.attrs = make([]string, 0, len(bat.Attrs)+len(ctr.attrs))
			for _, attr := range bat.Attrs {
				ctr.attrs = append(ctr.attrs, rName+"."+attr)
			}
			for _, attr := range ctr.bat.Attrs {
				ctr.attrs = append(ctr.attrs, sName+"."+attr)
			}
		} else {
			bat.Reorder(ctr.Probe.attrs)
		}
		{
			ctr.Probe.bat = batch.New(true, ctr.attrs)
			for i, attr := range bat.Attrs {
				vec := bat.GetVector(attr)
				ctr.Probe.bat.Vecs[i] = vector.New(vec.Typ)
			}
			j := len(bat.Attrs)
			for i, vec := range ctr.bat.Vecs {
				ctr.Probe.bat.Vecs[i+j] = vector.New(vec.Typ)
			}
		}
		if len(bat.Sels) > 0 {
			bat.Shuffle(proc)
		}
		if err := ctr.probeBatch(bat.Vecs, proc); err != nil {
			reg.Ch = nil
			reg.Wg.Done()
			bat.Clean(proc)
			return true, err
		}
		if ctr.Probe.bat.Vecs[0].Length() == 0 {
			reg.Wg.Done()
			bat.Clean(proc)
			continue
		}
		reg.Wg.Done()
		bat.Clean(proc)
		ctr.Probe.bat.Reduce(ctr.rattrs, proc)
		proc.Reg.InputBatch = ctr.Probe.bat
		ctr.Probe.bat = nil
		return false, nil
	}
}

func (ctr *Container) buildBatch(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.buildUnit(i, length, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) buildUnit(start, count int, vecs []*vector.Vector, proc *process.Process) error {
	var err error
	var matchs []int64

	copy(ctr.hashs[:count], OneUint64s[:count])
	ctr.fillHash(start, count, vecs[:ctr.n])
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for _, g := range gs {
					matchs, remaining = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs)
					for len(matchs) > 0 {
						for i, vec := range ctr.bat.Vecs {
							if err = vec.UnionOne(vecs[i], matchs[0], proc); err != nil {
								return err
							}
						}
						ctr.rows++
						matchs = matchs[1:]
						if proc.Size() > proc.Lim.Size {
							return errors.New("out of memory")
						}
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.BagGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewBagGroup(ctr.rows)
				{
					for i, vec := range ctr.bat.Vecs {
						if err = vec.UnionOne(vecs[i], remaining[0], proc); err != nil {
							return err
						}
					}
					if proc.Size() > proc.Lim.Size {
						return errors.New("out of memory")
					}
				}
				ctr.rows++
				ctr.groups[h] = append(ctr.groups[h], g)
				matchs, remaining = g.Fill(remaining[1:], ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs)
				for len(matchs) > 0 {
					for i, vec := range ctr.bat.Vecs {
						if err = vec.UnionOne(vecs[i], matchs[0], proc); err != nil {
							return err
						}
					}
					ctr.rows++
					matchs = matchs[1:]
					if proc.Size() > proc.Lim.Size {
						return errors.New("out of memory")
					}
				}
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
			}
			ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
		}
	}
	ctr.slots.Reset()
	return nil
}

func (ctr *Container) probeBatch(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.probeUnit(i, length, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) probeUnit(start, count int, vecs []*vector.Vector, proc *process.Process) error {
	var matchs []int64

	copy(ctr.hashs[:count], OneUint64s[:count])
	ctr.fillHash(start, count, vecs[:ctr.n])
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for k := 0; k < len(gs); k++ {
					g := gs[k]
					matchs, remaining = g.Probe(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs)
					if len(matchs) > 0 {
						if err := ctr.product(matchs, g, vecs, proc); err != nil {
							return err
						}
						if proc.Size() > proc.Lim.Size {
							return errors.New("out of memory")
						}
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			}
			ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
		}
	}
	ctr.slots.Reset()
	return nil
}

func (ctr *Container) product(sels []int64, g *hash.BagGroup, vecs []*vector.Vector, proc *process.Process) error {
	for _, sel := range sels {
		for _, gsel := range g.Sels {
			for i, vec := range vecs {
				if err := ctr.Probe.bat.Vecs[i].UnionOne(vec, sel, proc); err != nil {
					return err
				}
			}
			j := len(vecs)
			for i, vec := range ctr.bat.Vecs {
				if err := ctr.Probe.bat.Vecs[i+j].UnionOne(vec, gsel, proc); err != nil {
					return err
				}
			}
		}
	}
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
	if ctr.Probe.bat != nil {
		ctr.Probe.bat.Clean(proc)
		ctr.Probe.bat = nil
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
