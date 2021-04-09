package min

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/min"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewUint16(typ types.Type) *uint16Min {
	return &uint16Min{typ: typ}
}

func (a *uint16Min) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *uint16Min) Type() types.Type {
	return a.typ
}

func (a *uint16Min) Dup() aggregation.Aggregation {
	return &uint16Min{typ: a.typ}
}

func (a *uint16Min) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := min.Uint16MinSels(vec.Col.([]uint16), sels)
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := min.Uint16Min(vec.Col.([]uint16))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
	}
	return nil
}

func (a *uint16Min) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *uint16Min) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(2)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeUint16(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeUint16(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeUint16Slice(data[mempool.CountSize : mempool.CountSize+2])
	return vec, nil
}
