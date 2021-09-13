package group

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/aggregation/aggfunc"
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
	buf.WriteString("γ([")
	for i, g := range n.Gs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(g)
	}
	buf.WriteString("], [")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias))
	}
	buf.WriteString("]")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	{
		is := make([]int, len(n.Es))
		attrs := make([]string, 0, len(n.Gs)+len(n.Es))
		rattrs := make([]string, len(n.Gs)+len(n.Es))
		{
			mp := make(map[string]int)
			for i, g := range n.Gs {
				rattrs[i] = g
				if _, ok := mp[g]; !ok {
					mp[g] = len(attrs)
					attrs = append(attrs, g)
				}
			}
			for i, e := range n.Es {
				rattrs[i+len(n.Gs)] = e.Alias
				if _, ok := mp[e.Name]; !ok {
					mp[e.Name] = len(attrs)
					attrs = append(attrs, e.Name)
				}
				is[i] = mp[e.Name]
			}
		}
		n.Ctr = Container{
			is:     is,
			attrs:  attrs,
			rattrs: rattrs,
			refer:  n.Refer,
			n:      len(n.Gs),
			slots:  fastmap.New(),
			diffs:  make([]bool, UnitLimit),
			matchs: make([]int64, UnitLimit),
			hashs:  make([]uint64, UnitLimit),
			sels:   make([][]int64, UnitLimit),
			vec:    vector.New(types.Type{Oid: types.T_int8}),
		}
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	if proc.Reg.Ax == nil {
		ctr.clean(proc)
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		ctr.clean(proc)
		return false, nil
	}
	{
		ctr.rows = 0
		ctr.groups = make(map[uint64][]*hash.Group)
	}
	bat.Reorder(ctr.attrs)
	{
		ctr.bat = batch.New(true, n.Gs)
		for i, attr := range n.Gs {
			vec := bat.GetVector(attr)
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
		}
		for i, e := range n.Es {
			vec := bat.GetVector(ctr.attrs[ctr.is[i]])
			if e.Agg == nil {
				switch e.Op {
				case aggregation.Avg:
					e.Agg = aggfunc.NewAvg(vec.Typ)
				case aggregation.Max:
					e.Agg = aggfunc.NewMax(vec.Typ)
				case aggregation.Min:
					e.Agg = aggfunc.NewMin(vec.Typ)
				case aggregation.Sum:
					e.Agg = aggfunc.NewSum(vec.Typ)
				case aggregation.Count:
					e.Agg = aggfunc.NewCount(vec.Typ)
				case aggregation.StarCount:
					e.Agg = aggfunc.NewStarCount(vec.Typ)
				case aggregation.SumCount:
					e.Agg = aggfunc.NewSumCount(vec.Typ)
				default:
					bat.Clean(proc)
					ctr.clean(proc)
					return false, fmt.Errorf("unsupport aggregation operator '%v'", e.Op)
				}
				if e.Agg == nil {
					bat.Clean(proc)
					ctr.clean(proc)
					return false, fmt.Errorf("unsupport sumcount aggregation operator '%v' for %s", e.Op, bat.Vecs[i+len(n.Gs)].Typ)
				}
				n.Es[i].Agg = e.Agg
			}
		}
	}
	if len(bat.Sels) > 0 {
		bat.Shuffle(proc)
	}
	if err := ctr.batchGroup(bat.Vecs[:len(ctr.attrs)], n.Es, proc); err != nil {
		bat.Clean(proc)
		ctr.clean(proc)
		return false, err
	}
	bat.Clean(proc)
	vecs, err := ctr.eval(ctr.rows, n.Es, proc)
	if err != nil {
		ctr.clean(proc)
		return false, err
	}
	proc.Reg.Ax = &batch.Batch{
		Ro:    true,
		Attrs: ctr.rattrs,
		Vecs:  append(ctr.bat.Vecs, vecs...),
	}
	ctr.bat = nil
	ctr.clean(proc)
	return false, nil
}

func (ctr *Container) eval(length int64, es []aggregation.Extend, proc *process.Process) ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(es))
	for i, e := range es {
		typ := e.Agg.Type()
		vecs[i] = vector.New(typ)
		switch typ.Oid {
		case types.T_int8:
			data, err := proc.Alloc(length)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt8Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int8)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int16:
			data, err := proc.Alloc(length * 2)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt16Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int16)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int32:
			data, err := proc.Alloc(length * 4)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt32Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_int64:
			data, err := proc.Alloc(length * 8)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeInt64Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(int64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint8:
			data, err := proc.Alloc(length)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint8Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint8)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint16:
			data, err := proc.Alloc(length * 2)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint16Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint16)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint32:
			data, err := proc.Alloc(length * 4)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint32Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_uint64:
			data, err := proc.Alloc(length * 8)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeUint64Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(uint64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_float32:
			data, err := proc.Alloc(length * 4)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeFloat32Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(float32)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_float64:
			data, err := proc.Alloc(length * 8)
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			vs := encoding.DecodeFloat64Slice(data)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.(float64)
					}
				}
			}
			vecs[i].Col = vs
			vecs[i].Data = data
		case types.T_char, types.T_varchar:
			size := 0
			vs := make([][]byte, length)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.([]byte)
						size += len(vs[g.Sel])
					}
				}
			}
			data, err := proc.Alloc(int64(size))
			if err != nil {
				for j := 0; j < i; j++ {
					vecs[j].Free(proc)
				}
				return nil, err
			}
			col := vecs[i].Col.(*types.Bytes)
			{
				o := uint32(0)
				col.Data = data[:0]
				col.Offsets = make([]uint32, 0, length)
				col.Lengths = make([]uint32, 0, length)
				for _, v := range vs {
					col.Offsets = append(col.Offsets, o)
					col.Lengths = append(col.Lengths, uint32(len(v)))
					o += uint32(len(v))
					col.Data = append(col.Data, v...)
				}
			}
			vecs[i].Col = col
			vecs[i].Data = data
		case types.T_tuple:
			vs := make([][]interface{}, length)
			for _, gs := range ctr.groups {
				for _, g := range gs {
					if v := g.Aggs[i].Eval(); v == nil {
						vecs[i].Nsp.Add(uint64(g.Sel))
					} else {
						vs[g.Sel] = v.([]interface{})
					}
				}
			}
			vecs[i].Col = vs
		}
	}
	for i, e := range es {
		vecs[i].Ref = ctr.refer[e.Alias]
	}
	return vecs, nil
}

func (ctr *Container) batchGroup(vecs []*vector.Vector, es []aggregation.Extend, proc *process.Process) error {
	for i, j := 0, vecs[0].Length(); i < j; i += UnitLimit {
		length := j - i
		if length > UnitLimit {
			length = UnitLimit
		}
		if err := ctr.unitGroup(i, length, vecs, es, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) unitGroup(start int, count int, vecs []*vector.Vector, es []aggregation.Extend, proc *process.Process) error {
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
				ctr.groups[h] = make([]*hash.Group, 0, 8)
			}
			for len(remaining) > 0 {
				g := hash.NewGroup(ctr.rows, ctr.is, es)
				for i, vec := range ctr.bat.Vecs {
					if err = vec.UnionOne(vecs[i], remaining[0], proc); err != nil {
						return err
					}
				}
				ctr.rows++
				ctr.groups[h] = append(ctr.groups[h], g)
				remaining = g.Fill(remaining, ctr.matchs, vecs, ctr.bat.Vecs[:ctr.n], ctr.diffs, proc)
				copy(ctr.diffs[:len(remaining)], ZeroBools[:len(remaining)])
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
}
