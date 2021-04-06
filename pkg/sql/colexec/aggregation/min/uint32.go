package min

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vectorize/min"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func NewUint32(typ types.Type) *uint32Min {
	return &uint32Min{typ: typ}
}

func (a *uint32Min) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *uint32Min) Type() types.Type {
	return a.typ
}

func (a *uint32Min) Dup() aggregation.Aggregation {
	return &uint32Min{typ: a.typ}
}

func (a *uint32Min) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := min.Uint32MinSels(vec.Col.([]uint32), sels)
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := min.Uint32Min(vec.Col.([]uint32))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
	}
	return nil
}

func (a *uint32Min) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *uint32Min) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(4)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeUint32(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeUint32(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeUint32Slice(data[mempool.CountSize : mempool.CountSize+4])
	return vec, nil
}
