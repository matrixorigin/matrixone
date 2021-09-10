package max

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/max"
	"matrixone/pkg/vm/process"
)

func NewFloat32(typ types.Type) *float32Max {
	return &float32Max{typ: typ}
}

func (a *float32Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *float32Max) Type() types.Type {
	return a.typ
}

func (a *float32Max) Dup() aggregation.Aggregation {
	return &float32Max{typ: a.typ}
}

func (a *float32Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Float32MaxSels(vec.Col.([]float32), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Float32Max(vec.Col.([]float32))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *float32Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *float32Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(4)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	vs := encoding.DecodeFloat32Slice(data[:4])
	vs[0] = a.v
	if a.cnt == 0 {
		vec.Nsp.Add(0)
	}
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
