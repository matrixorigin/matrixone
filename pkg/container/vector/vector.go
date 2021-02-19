package vector

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

var ConstTrue, ConstFalse *Vector

func init() {
	ConstTrue = &Vector{Typ: types.T_bool, Col: []bool{true}, Nsp: &nulls.Nulls{}}
	ConstFalse = &Vector{Typ: types.T_bool, Col: []bool{false}, Nsp: &nulls.Nulls{}}
}

func New(typ types.T) *Vector {
	switch typ {
	case types.T_int:
		return &Vector{
			Typ: typ,
			Col: []int64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_sel:
		return &Vector{
			Typ: typ,
			Col: []int64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_bool:
		return &Vector{
			Typ: typ,
			Col: []bool{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_float:
		return &Vector{
			Typ: typ,
			Col: []float64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_tuple:
		return &Vector{
			Typ: typ,
			Nsp: &nulls.Nulls{},
			Col: [][]interface{}{},
		}
	case types.T_bytes, types.T_json:
		return &Vector{
			Typ: typ,
			Col: &Bytes{},
			Nsp: &nulls.Nulls{},
		}
	default:
		panic(fmt.Errorf("unsupport type %s", typ))
	}
}

func (v *Vector) Reset() {
	switch v.Typ {
	case types.T_int:
		v.Col = v.Col.([]int64)[:0]
	case types.T_bool:
		v.Col = v.Col.([]bool)[:0]
	case types.T_float:
		v.Col = v.Col.([]float64)[:0]
	case types.T_bytes, types.T_json:
		v.Col.(*Bytes).Reset()
	case types.T_tuple:
		v.Col = v.Col.([][]interface{})[:0]
	default:
		panic(fmt.Errorf("unsupport type %s", v.Typ))
	}
}

func (v *Vector) Free(p *process.Process) {
	if v.Data != nil {
		p.Mp.Free(v.Data)
		if encoding.DecodeUint64(v.Data[:mempool.CountSize]) == 0 {
			p.Free(int64(cap(v.Data)))
		}
	}
}

func (v *Vector) SetCol(col interface{}) {
	v.Col = col
}

func (v *Vector) Bools() []bool {
	if v.Col == nil {
		return []bool{}
	}
	return v.Col.([]bool)
}

func (v *Vector) Ints() []int64 {
	if v.Col == nil {
		return []int64{}
	}
	return v.Col.([]int64)
}

func (v *Vector) Sels() []int64 {
	if v.Col == nil {
		return []int64{}
	}
	return v.Col.([]int64)
}

func (v *Vector) Floats() []float64 {
	if v.Col == nil {
		return []float64{}
	}
	return v.Col.([]float64)
}

func (v *Vector) Bytes() *Bytes {
	return v.Col.(*Bytes)
}

func (v *Vector) Tuples() [][]interface{} {
	if v.Col == nil {
		return [][]interface{}{}
	}
	return v.Col.([][]interface{})
}

func (v *Vector) Length() int {
	switch v.Typ {
	case types.T_int:
		return len(v.Col.([]int64))
	case types.T_sel:
		return len(v.Col.([]int64))
	case types.T_bool:
		return len(v.Col.([]bool))
	case types.T_float:
		return len(v.Col.([]float64))
	case types.T_bytes, types.T_json:
		return len(v.Col.(*Bytes).Os)
	case types.T_tuple:
		return len(v.Col.([][]interface{}))
	default:
		panic(fmt.Errorf("unsupport type %s", v.Typ))
	}
}

func (v *Vector) Window(start, end int) *Vector {
	switch v.Typ {
	case types.T_int:
		return &Vector{
			Typ: v.Typ,
			Col: v.Col.([]int64)[start:end],
			Nsp: v.Nsp.Range(uint64(start), uint64(end)),
		}
	case types.T_bool:
		return &Vector{
			Typ: v.Typ,
			Col: v.Col.([]bool)[start:end],
			Nsp: v.Nsp.Range(uint64(start), uint64(end)),
		}
	case types.T_float:
		return &Vector{
			Typ: v.Typ,
			Col: v.Col.([]float64)[start:end],
			Nsp: v.Nsp.Range(uint64(start), uint64(end)),
		}
	case types.T_bytes, types.T_json:
		return &Vector{
			Typ: v.Typ,
			Col: v.Col.(*Bytes).Window(start, end),
			Nsp: v.Nsp.Range(uint64(start), uint64(end)),
		}
	case types.T_tuple:
		return &Vector{
			Typ: v.Typ,
			Col: v.Col.([][]interface{})[start:end],
			Nsp: v.Nsp.Range(uint64(start), uint64(end)),
		}
	default:
		panic(fmt.Errorf("unsupport type %s", v.Typ))
	}
}

func (v *Vector) Append(arg interface{}) error {
	switch v.Typ {
	case types.T_int:
		col := v.Col.([]int64)
		col = append(col, arg.([]int64)...)
		v.Col = col
	case types.T_bool:
		col := v.Col.([]bool)
		col = append(col, arg.([]bool)...)
		v.Col = col
	case types.T_float:
		col := v.Col.([]float64)
		col = append(col, arg.([]float64)...)
		v.Col = col
	case types.T_bytes, types.T_json:
		return v.Col.(*Bytes).Append(arg.([][]byte))
	case types.T_tuple:
		col := v.Col.([][]interface{})
		col = append(col, arg.([][]interface{})...)
		v.Col = col
	default:
		return fmt.Errorf("unsupport type %s", v.Typ)
	}
	return nil
}

func (v *Vector) Filter(sels []int64) *Vector {
	switch v.Typ {
	case types.T_int:
		vs := v.Col.([]int64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = v.Nsp.Filter(sels)
	case types.T_bool:
		vs := v.Col.([]bool)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = v.Nsp.Filter(sels)
	case types.T_float:
		vs := v.Col.([]float64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = v.Nsp.Filter(sels)
	case types.T_bytes, types.T_json:
		col := v.Col.(*Bytes)
		os, ns := col.Os, col.Ns
		for i, sel := range sels {
			os[i] = os[sel]
			ns[i] = ns[sel]
		}
		col.Os = os[:len(sels)]
		col.Ns = ns[:len(sels)]
		v.Nsp = v.Nsp.Filter(sels)
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = v.Nsp.Filter(sels)
	default:
		panic(fmt.Errorf("unsupport type %s", v.Typ))
	}
	return v
}

func (v *Vector) Show() ([]byte, error) {
	var buf bytes.Buffer

	switch v.Typ {
	case types.T_int:
		buf.WriteByte(byte(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt64Slice(v.Col.([]int64)))
		return buf.Bytes(), nil
	case types.T_bool:
		buf.WriteByte(byte(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeBoolSlice(v.Col.([]bool)))
		return buf.Bytes(), nil
	case types.T_float:
		buf.WriteByte(byte(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeFloat64Slice(v.Col.([]float64)))
		return buf.Bytes(), nil
	case types.T_bytes, types.T_json:
		buf.WriteByte(byte(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		Col := v.Col.(*Bytes)
		cnt := int32(len(Col.Os))
		buf.Write(encoding.EncodeInt32(cnt))
		if cnt == 0 {
			return buf.Bytes(), nil
		}
		buf.Write(encoding.EncodeUint32Slice(Col.Ns))
		buf.Write(Col.Data)
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupport encoding type %s", v.Typ)
	}
}

func (v *Vector) Read(data []byte, p *process.Process) error {
	if err := p.Alloc(int64(cap(data))); err != nil {
		return err
	}
	v.Data = data
	data = data[mempool.CountSize:]
	switch typ := types.T(data[0]); typ {
	case types.T_int:
		size := encoding.DecodeUint32(data[1:])
		if size == 0 {
			v.Col = encoding.DecodeInt64Slice(data[5:])
		} else {
			data = data[5:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeInt64Slice(data[size:])
		}
	case types.T_bool:
		size := encoding.DecodeUint32(data[1:])
		if size == 0 {
			v.Col = encoding.DecodeBoolSlice(data[5:])
		} else {
			data = data[5:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeBoolSlice(data[size:])
		}
	case types.T_float:
		size := encoding.DecodeUint32(data[1:])
		if size == 0 {
			v.Col = encoding.DecodeFloat64Slice(data[5:])
		} else {
			data = data[5:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeFloat64Slice(data[size:])
		}
	case types.T_bytes, types.T_json:
		Col := v.Col.(*Bytes)
		Col.Reset()
		size := encoding.DecodeUint32(data[1:])
		data = data[5:]
		if size > 0 {
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}
		cnt := encoding.DecodeInt32(data)
		if cnt == 0 {
			break
		}
		data = data[4:]
		Col.Os = make([]uint32, cnt)
		Col.Ns = encoding.DecodeUint32Slice(data[: 4*cnt : 4*cnt])
		Col.Data = data[4*cnt:]
		{
			o := uint32(0)
			for i, n := range Col.Ns {
				Col.Os[i] = o
				o += n
			}
		}
	default:
		return fmt.Errorf("unsupport decoding type %s", typ)
	}
	return nil
}

func (v *Vector) String() string {
	switch v.Typ {
	case types.T_int:
		col := v.Col.([]int64)
		if len(col) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_bool:
		col := v.Col.([]bool)
		if len(col) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_float:
		col := v.Col.([]float64)
		if len(col) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_bytes:
		col := v.Col.(*Bytes)
		if len(col.Os) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%s", col.Data[:col.Ns[0]])
			}
		}
	case types.T_json:
		col := v.Col.(*Bytes)
		if len(col.Os) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%s", col.Data[:col.Ns[0]])
			}
		}
	case types.T_tuple:
		col := v.Col.([][]interface{})
		if len(col) == 1 {
			if v.Nsp.Contains(0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	}
	return fmt.Sprintf("%v-%s", v.Col, v.Nsp)
}
