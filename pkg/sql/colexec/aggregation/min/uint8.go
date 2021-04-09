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

func NewUint8(typ types.Type) *uint8Min {
	return &uint8Min{typ: typ}
}

func (a *uint8Min) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *uint8Min) Type() types.Type {
	return a.typ
}

func (a *uint8Min) Dup() aggregation.Aggregation {
	return &uint8Min{typ: a.typ}
}

func (a *uint8Min) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := min.Uint8MinSels(vec.Col.([]uint8), sels)
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := min.Uint8Min(vec.Col.([]uint8))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
	}
	return nil
}

func (a *uint8Min) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *uint8Min) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(1)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeUint8(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeUint8(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeUint8Slice(data[mempool.CountSize : mempool.CountSize+1])
	return vec, nil
}
