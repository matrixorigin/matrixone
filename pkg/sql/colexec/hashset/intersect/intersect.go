package intersect

import (
	"bytes"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/hash"
	"matrixbase/pkg/intmap/fastmap"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func init() {
	ZeroBools = make([]bool, UnitLimit)
	OneUint64s = make([]uint64, UnitLimit)
	for i := range OneUint64s {
		OneUint64s[i] = 1
	}
}

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("R ∩ S")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = Container{
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

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if !ctr.builded {
		if err := ctr.build(proc); err != nil {
			return false, err
		}
		ctr.builded = true
	}
	return ctr.probe(proc)
}

// R ∩ S - s is the smaller relation
func (ctr *Container) build(proc *process.Process) error {
	var err error

	reg := proc.Reg.Ws[1]
	for {
		v := <-reg.Ch
		if v == nil {
			break
		}
		bat := v.(*batch.Batch)
		if err = bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
			reg.Wg.Done()
			return err
		}
		ctr.bats = append(ctr.bats, bat)
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
	reg.Wg.Done()
	return nil
}

func (ctr *Container) probe(proc *process.Process) (bool, error) {
	reg := proc.Reg.Ws[0]
	defer reg.Wg.Done()
	v := <-reg.Ch
	if v == nil {
		proc.Reg.Ax = nil
		ctr.clean(nil, proc)
		return true, nil
	}
	bat := v.(*batch.Batch)
	if len(ctr.groups) == 0 {
		reg.Ch = nil
		proc.Reg.Ax = nil
		ctr.clean(bat, proc)
		return true, nil
	}
	if len(bat.Sels) == 0 {
		if err := ctr.probeBatch(bat.Vecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
		bat.Sels = ctr.probeState.sels
		bat.SelsData = ctr.probeState.data
		ctr.probeState.sels = nil
		ctr.probeState.data = nil
		proc.Reg.Ax = bat
	} else {
		if err := ctr.probeBatchSels(bat.Sels, bat.Vecs, proc); err != nil {
			ctr.clean(bat, proc)
			return false, err
		}
		bat.Sels, ctr.probeState.sels = ctr.probeState.sels, bat.Sels
		bat.SelsData, ctr.probeState.data = ctr.probeState.data, bat.SelsData
		ctr.probeState.sels = ctr.probeState.sels[:0] // reset
		proc.Reg.Ax = bat
	}
	return false, nil
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
	var sel int64
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
					if sel, remaining, err = g.Probe(remaining, ctr.matchs, vecs, ctr.bats, ctr.diffs, proc); err != nil {
						return err
					}
					if sel >= 0 {
						gs = append(gs[:k], gs[k+1:]...)
						k--
						if len(gs) == 0 {
							delete(ctr.groups, h)
						}
						{
							if n := cap(ctr.probeState.sels); n == 0 {
								data, err := proc.Alloc(int64(8 * 8))
								if err != nil {
									return err
								}
								newsels := encoding.DecodeInt64Slice(data)
								ctr.probeState.data = data
								ctr.probeState.sels = newsels[:0]
							} else if n == len(ctr.probeState.sels) {
								if n < 1024 {
									n *= 2
								} else {
									n += n / 4
								}
								data, err := proc.Alloc(int64(n * 8))
								if err != nil {
									return err
								}
								newsels := encoding.DecodeInt64Slice(data)
								copy(newsels, ctr.probeState.sels)
								ctr.probeState.sels = newsels[:n]
								proc.Free(ctr.probeState.data)
								ctr.probeState.data = data
							}
							ctr.probeState.sels = append(ctr.probeState.sels, sel)
						}
					}
					copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
				}
				ctr.sels[ctr.slots.Vs[i][j]] = ctr.sels[ctr.slots.Vs[i][j]][:0]
			}
		}
	}
	ctr.slots.Reset()
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
	ctr.hashs = ctr.hashs[:count]
	for _, vec := range vecs {
		hash.RehashSels(count, sels, ctr.hashs, vec)
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
	if data := ctr.probeState.data; data != nil {
		proc.Free(data)
	}
	for _, bat := range ctr.bats {
		bat.Clean(proc)
	}
	register.FreeRegisters(proc)
}
