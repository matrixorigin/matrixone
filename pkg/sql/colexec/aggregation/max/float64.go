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

func NewFloat64(typ types.Type) *float64Max {
	return &float64Max{typ: typ}
}

func (a *float64Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *float64Max) Type() types.Type {
	return a.typ
}

func (a *float64Max) Dup() aggregation.Aggregation {
	return &float64Max{typ: a.typ}
}

func (a *float64Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Float64MaxSels(vec.Col.([]float64), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Float64Max(vec.Col.([]float64))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *float64Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *float64Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		vs := []float64{0}
		copy(data[mempool.CountSize:], encoding.EncodeFloat64Slice(vs))
		vec.Col = vs
	} else {
		vs := []float64{a.v}
		copy(data[mempool.CountSize:], encoding.EncodeFloat64Slice(vs))
		vec.Col = vs
	}
	vec.Data = data
	return vec, nil
}
