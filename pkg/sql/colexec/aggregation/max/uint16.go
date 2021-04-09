package max

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/max"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewUint16(typ types.Type) *uint16Max {
	return &uint16Max{typ: typ}
}

func (a *uint16Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *uint16Max) Type() types.Type {
	return a.typ
}

func (a *uint16Max) Dup() aggregation.Aggregation {
	return &uint16Max{typ: a.typ}
}

func (a *uint16Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Uint16MaxSels(vec.Col.([]uint16), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Uint16Max(vec.Col.([]uint16))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *uint16Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *uint16Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
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
