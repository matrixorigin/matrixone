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

func NewFloat32(typ types.Type) *float32Min {
	return &float32Min{typ: typ}
}

func (a *float32Min) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *float32Min) Type() types.Type {
	return a.typ
}

func (a *float32Min) Dup() aggregation.Aggregation {
	return &float32Min{typ: a.typ}
}

func (a *float32Min) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := min.Float32MinSels(vec.Col.([]float32), sels)
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := min.Float32Min(vec.Col.([]float32))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v < a.v {
			a.v = v
		}
	}
	return nil
}

func (a *float32Min) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *float32Min) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(4)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeFloat32(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeFloat32(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeFloat32Slice(data[mempool.CountSize : mempool.CountSize+4])
	return vec, nil
}
