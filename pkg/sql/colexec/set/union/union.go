package union

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
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
	buf.WriteString(fmt.Sprintf("%s ∪  %s", n.R, n.S))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		probed:  false,
		builded: false,
		diffs:   make([]bool, UnitLimit),
		matchs:  make([]int64, UnitLimit),
		hashs:   make([]uint64, UnitLimit),
		sels:    make([][]int64, UnitLimit),
		groups:  make(map[uint64][]*hash.SetGroup),
		slots:   fastmap.Pool.Get().(*fastmap.Map),
	}
	return nil
}

// sql union all is just a fake bag union
func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if !ctr.builded {
		if err := ctr.build(proc); err != nil {
			return true, err
		}
		ctr.builded = true
	}
	return ctr.probe(proc)
}

// R ∪  S - S is the smaller relation
func (ctr *Container) build(proc *process.Process) error {
	var err error

	reg := proc.Reg.Ws[1]
	for {
		v := <-reg.Ch
		if v == nil {
			reg.Wg.Done()
			break
		}
		bat := v.(*batch.Batch)
		if bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		if err = bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
			reg.Wg.Done()
			return err
		}
		ctr.bats = append(ctr.bats, bat)
		ctr.state.gs = append(ctr.state.gs, make([]*hash.SetGroup, 0, 8))
		if len(bat.Sels) == 0 {
			if err = ctr.buildBatch(bat.Vecs, proc); err != nil {
				reg.Wg.Done()
				return err
			}
		} else {
			if err = ctr.buildBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
				reg.Wg.Done()
				return err
			}
		}
		reg.Wg.Done()
	}
	return nil
}

func (ctr *Container) probe(proc *process.Process) (bool, error) {
	if !ctr.probed {
		for {
			reg := proc.Reg.Ws[0]
			v := <-reg.Ch
			if v == nil {
				reg.Wg.Done()
				proc.Reg.Ax = nil
				ctr.clean(nil, proc)
				break
			}
			bat := v.(*batch.Batch)
			if bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if len(ctr.groups) == 0 {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ax = nil
				ctr.clean(bat, proc)
				break
			}
			if len(bat.Sels) == 0 {
				if err := ctr.probeBatch(bat.Vecs, proc); err != nil {
					reg.Wg.Done()
					ctr.clean(bat, proc)
					return true, err
				}
				bat.Sels = ctr.state.sels
				bat.SelsData = ctr.state.data
				ctr.state.sels = nil
				ctr.state.data = nil
			} else {
				if err := ctr.probeBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
					reg.Wg.Done()
					ctr.clean(bat, proc)
					return true, err
				}
				bat.Sels, ctr.state.sels = ctr.state.sels, bat.Sels
				bat.SelsData, ctr.state.data = ctr.state.data, bat.SelsData
				ctr.state.sels = ctr.state.sels[:0] // reset
			}
			if len(bat.Sels) == 0 {
				reg.Wg.Done()
				bat.Clean(proc)
				continue
			}
			reg.Wg.Done()
			proc.Reg.Ax = bat
			return false, nil
		}
		ctr.probed = true
	}
	for {
		if len(ctr.bats) == 0 {
			proc.Reg.Ax = nil
			ctr.clean(nil, proc)
			return true, nil
		}
		bat := ctr.bats[0]
		for _, g := range ctr.state.gs[0] {
			if g.Sel >= 0 {
				if err := ctr.expan(1, proc); err != nil {
					ctr.clean(nil, proc)
					return true, err
				}
				ctr.state.sels = append(ctr.state.sels, g.Sel)
			}
		}
		if len(ctr.state.sels) == 0 {
			bat.Clean(proc)
			ctr.bats = ctr.bats[1:]
			ctr.state.gs = ctr.state.gs[1:]
			continue
		}
		if bat.SelsData != nil {
			proc.Free(bat.SelsData)
		}
		bat.Sels = ctr.state.sels
		bat.SelsData = ctr.state.data
		ctr.state.sels = nil
		ctr.state.data = nil
		proc.Reg.Ax = bat
		ctr.bats = ctr.bats[1:]
		ctr.state.gs = ctr.state.gs[1:]
		return false, nil
	}
}

func (ctr *Container) buildBatch(vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.buildUnit(i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) buildBatchSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.buildUnit(0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) buildUnit(start, count int, sels []int64,
	vecs []*vector.Vector, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(start, count, vecs)
		} else {
			ctr.fillHashSels(count, sels, vecs)
		}
	}
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for _, g := range gs {
					if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			} else {
				ctr.groups[h] = make([]*hash.SetGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewSetGroup(int64(len(ctr.bats)-1), int64(remaining[0]))
				ctr.state.gs[len(ctr.bats)-1] = append(ctr.state.gs[len(ctr.bats)-1], g)
				ctr.groups[h] = append(ctr.groups[h], g)
				if remaining, err = g.Fill(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
					return err
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
		if err := ctr.probeUnit(i, length, nil, vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) probeBatchSels(sels []int64, vecs []*vector.Vector, proc *process.Process) error {
	for i, j := 0, len(sels); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.probeUnit(0, length, sels[i:i+length], vecs, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) probeUnit(start, count int, sels []int64,
	vecs []*vector.Vector, proc *process.Process) error {
	var err error

	{
		copy(ctr.hashs[:count], OneUint64s[:count])
		if len(sels) == 0 {
			ctr.fillHash(start, count, vecs)
		} else {
			ctr.fillHashSels(count, sels, vecs)
		}
	}
	copy(ctr.diffs[:count], ZeroBools[:count])
	for i, hs := range ctr.slots.Ks {
		for j, h := range hs {
			remaining := ctr.sels[ctr.slots.Vs[i][j]]
			if gs, ok := ctr.groups[h]; ok {
				for k := 0; k < len(gs); k++ {
					g := gs[k]
					if _, remaining, err = g.Probe(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
			}
			ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
			if n := len(remaining); n > 0 { // output
				if err := ctr.expan(n, proc); err != nil {
					return err
				}
				ctr.state.sels = append(ctr.state.sels, remaining...)
			}
		}
	}
	ctr.slots.Reset()
	return nil
}

func (ctr *Container) expan(cnt int, proc *process.Process) error {
	n := int64(cap(ctr.state.sels))
	size := int64(len(ctr.state.sels) + cnt)
	if n >= size {
		return nil
	}
	if n == 0 {
		data, err := proc.Alloc(size * 8)
		if err != nil {
			return err
		}
		newsels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+size*8])
		ctr.state.data = data
		ctr.state.sels = newsels[:0]
		return nil
	}
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(ctr.state.sels) < 1024 {
			newcap = size
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	data, err := proc.Alloc(newcap * 8)
	if err != nil {
		return err
	}
	newsels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+newcap*8])
	copy(newsels, ctr.state.sels)
	ctr.state.sels = newsels[:size-int64(cnt)]
	proc.Free(ctr.state.data)
	ctr.state.data = data
	return nil
}

func (ctr *Container) fillHash(start, count int, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, ctr.hashs, vec)
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

func (ctr *Container) fillHashSels(count int, sels []int64, vecs []*vector.Vector) {
	var cnt int64

	{
		for i, sel := range sels {
			if i == 0 || sel > cnt {
				cnt = sel
			}
		}
	}
	ctr.hashs = ctr.hashs[:cnt+1]
	for _, vec := range vecs {
		hash.RehashSels(sels[:count], ctr.hashs, vec)
	}
	nextslot := 0
	for i, h := range ctr.hashs {
		slot, ok := ctr.slots.Get(h)
		if !ok {
			slot = nextslot
			ctr.slots.Set(h, slot)
			nextslot++
		}
		ctr.sels[slot] = append(ctr.sels[slot], sels[i])
	}
}

func (ctr *Container) clean(bat *batch.Batch, proc *process.Process) {
	if bat != nil {
		bat.Clean(proc)
	}
	fastmap.Pool.Put(ctr.slots)
	if data := ctr.state.data; data != nil {
		proc.Free(data)
	}
	for _, bat := range ctr.bats {
		bat.Clean(proc)
	}
}
