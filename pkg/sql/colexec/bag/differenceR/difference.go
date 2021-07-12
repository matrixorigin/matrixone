package differenceR

import (
	"bytes"
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/block"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/google/uuid"
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
	buf.WriteString(fmt.Sprintf("%s - %s", n.R, n.S))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
		diffs:  make([]bool, UnitLimit),
		matchs: make([]int64, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		sels:   make([][]int64, UnitLimit),
		groups: make(map[uint64][]*hash.BagGroup),
		slots:  fastmap.Pool.Get().(*fastmap.Map),
	}
	ctr := &n.Ctr
	uuid, err := uuid.NewUUID()
	if err != nil {
		fastmap.Pool.Put(ctr.slots)
		return err
	}
	ctr.spill.id = fmt.Sprintf("%s.%v", proc.Id, uuid)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			ctr.spill.e = n.E
			if err := ctr.build(proc); err != nil {
				ctr.clean(proc)
				return true, err
			}
			ctr.state = Probe
		case Probe:
			if err := ctr.probe(proc); err != nil {
				ctr.clean(proc)
				return true, err
			}
			ctr.state = Eval
		case Eval:
			return ctr.eval(proc)
		}
	}
	/*
		if !ctr.builded {
			if err := ctr.build(proc); err != nil {
				return true, err
			}
			ctr.builded = true
		}
		return ctr.probe(proc)
	*/
}

// S - R - s is the smaller relation
func (ctr *Container) build(proc *process.Process) error {
	var err error

	reg := proc.Reg.Ws[0]
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
		if ctr.spill.attrs == nil {
			ctr.spill.attrs = make([]string, len(bat.Attrs))
			for i, attr := range bat.Attrs {
				ctr.spill.attrs[i] = attr
			}
			ctr.spill.cs = make([]uint64, len(bat.Attrs))
			ctr.spill.md = make([]metadata.Attribute, len(bat.Attrs))
			for i, attr := range bat.Attrs {
				vec, err := bat.GetVector(attr, proc)
				if err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
				ctr.spill.md[i] = metadata.Attribute{
					Name: attr,
					Type: vec.Typ,
					Alg:  compress.Lz4,
				}
				ctr.spill.cs[i] = encoding.DecodeUint64(vec.Data[:mempool.CountSize])
			}
		} else {
			bat.Reorder(ctr.spill.attrs)
		}
		if err = bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
			reg.Ch = nil
			reg.Wg.Done()
			bat.Clean(proc)
			return err
		}
		ctr.bats = append(ctr.bats, &block.Block{
			Bat:   bat,
			R:     ctr.spill.r,
			Cs:    ctr.spill.cs,
			Attrs: ctr.spill.attrs,
		})
		ctr.probeState.gs = append(ctr.probeState.gs, make([]*hash.BagGroup, 0, 8))
		if len(bat.Sels) == 0 {
			if err = ctr.buildBatch(bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				return err
			}
		} else {
			if err = ctr.buildBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				return err
			}
		}
		/*
			switch {
			case ctr.spilled:
				blk := ctr.bats[len(ctr.bats)-1]
				if err := blk.Bat.Prefetch(blk.Bat.Attrs, blk.Bat.Vecs, proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					return err
				}
				if err := ctr.spill.r.Write(blk.Bat); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					return err
				}
				blk.Seg = ctr.spill.r.Segments()[len(ctr.bats)-1]
				blk.Bat.Clean(proc)
				blk.Bat = nil
			case proc.Size() > proc.Lim.Size:
				if err := ctr.newSpill(proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					return err
				}
				for i, blk := range ctr.bats {
					if err := blk.Bat.Prefetch(blk.Bat.Attrs, blk.Bat.Vecs, proc); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						return err
					}
					if err := ctr.spill.r.Write(blk.Bat); err != nil {
						reg.Ch = nil
						reg.Wg.Done()
						return err
					}
					blk.R = ctr.spill.r
					blk.Seg = ctr.spill.r.Segments()[i]
					blk.Bat.Clean(proc)
					blk.Bat = nil
				}
				ctr.spilled = true
			}
		*/
		reg.Wg.Done()
	}
	return nil
}

func (ctr *Container) probe(proc *process.Process) error {
	reg := proc.Reg.Ws[1]
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
		if len(ctr.groups) == 0 {
			reg.Ch = nil
			reg.Wg.Done()
			bat.Clean(proc)
			break
		}
		if len(bat.Sels) == 0 {
			if err := ctr.probeBatch(bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
		} else {
			if err := ctr.probeBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
		}
		reg.Wg.Done()
		bat.Clean(proc)
	}
	return nil
}

func (ctr *Container) eval(proc *process.Process) (bool, error) {
	for {
		if len(ctr.bats) == 0 {
			proc.Reg.Ax = nil
			ctr.clean(proc)
			return true, nil
		}
		bat, err := ctr.bats[0].GetBatch(proc)
		if err != nil {
			ctr.clean(proc)
			return true, err
		}
		for _, g := range ctr.probeState.gs[0] {
			if len(g.Sels) > 0 {
				if err := ctr.expan(len(g.Sels), proc); err != nil {
					ctr.clean(proc)
					return true, err
				}
				ctr.probeState.sels = append(ctr.probeState.sels, g.Sels...)
			}
		}
		if len(ctr.probeState.sels) == 0 {
			bat.Clean(proc)
			ctr.bats = ctr.bats[1:]
			ctr.probeState.gs = ctr.probeState.gs[1:]
			continue
		}
		if bat.SelsData != nil {
			proc.Free(bat.SelsData)
		}
		bat.Sels = ctr.probeState.sels
		bat.SelsData = ctr.probeState.data
		ctr.probeState.sels = nil
		ctr.probeState.data = nil
		proc.Reg.Ax = bat
		ctr.bats = ctr.bats[1:]
		ctr.probeState.gs = ctr.probeState.gs[1:]
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
				ctr.groups[h] = make([]*hash.BagGroup, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewBagGroup(int64(len(ctr.bats)-1), int64(remaining[0]))
				ctr.probeState.gs[len(ctr.bats)-1] = append(ctr.probeState.gs[len(ctr.bats)-1], g)
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
	var matchs []int64

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
					if matchs, remaining, err = g.Probe(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					if len(matchs) > len(g.Is) {
						g.Is = g.Is[:0]
						g.Sels = g.Sels[:0]
					} else {
						g.Is = g.Is[len(matchs):]
						g.Sels = g.Sels[len(matchs):]
					}
					if len(g.Is) == 0 {
						g.Free(proc)
						gs = append(gs[:k], gs[k+1:]...)
						k--
						if len(gs) == 0 {
							delete(ctr.groups, h)
						} else {
							ctr.groups[h] = gs
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

func (ctr *Container) expan(cnt int, proc *process.Process) error {
	n := int64(cap(ctr.probeState.sels))
	size := int64(len(ctr.probeState.sels) + cnt)
	if n >= size {
		return nil
	}
	if n == 0 {
		data, err := proc.Alloc(size * 8)
		if err != nil {
			return err
		}
		newsels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+size*8])
		ctr.probeState.data = data
		ctr.probeState.sels = newsels[:0]
		return nil
	}
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(ctr.probeState.sels) < 1024 {
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
	copy(newsels, ctr.probeState.sels)
	ctr.probeState.sels = newsels[:size-int64(cnt)]
	proc.Free(ctr.probeState.data)
	ctr.probeState.data = data
	return nil
}

func (ctr *Container) newSpill(proc *process.Process) error {
	var defs []engine.TableDef

	for _, attr := range ctr.spill.md {
		defs = append(defs, &engine.AttributeDef{attr})
	}
	if err := ctr.spill.e.Create(ctr.spill.id, defs, nil, nil, ""); err != nil {
		return err
	}
	r, err := ctr.spill.e.Relation(ctr.spill.id)
	if err != nil {
		ctr.spill.e.Delete(ctr.spill.id)
		return err
	}
	ctr.spill.r = r
	return nil
}

func (ctr *Container) fillHash(start, count int, vecs []*vector.Vector) {
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.Rehash(count, ctr.hashs, vec.Window(start, start+count))
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

func (ctr *Container) clean(proc *process.Process) {
	fastmap.Pool.Put(ctr.slots)
	if data := ctr.probeState.data; data != nil {
		proc.Free(data)
		ctr.probeState.data = nil
		ctr.probeState.sels = nil
	}
	for _, blk := range ctr.bats {
		if blk.Bat != nil {
			blk.Bat.Clean(proc)
			blk.Bat = nil
		}
	}
	if ctr.spill.r != nil {
		ctr.spill.e.Delete(ctr.spill.id)
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
