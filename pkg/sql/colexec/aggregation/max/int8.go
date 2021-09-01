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

func NewInt8(typ types.Type) *int8Max {
	return &int8Max{typ: typ}
}

func (a *int8Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *int8Max) Type() types.Type {
	return a.typ
}

func (a *int8Max) Dup() aggregation.Aggregation {
	return &int8Max{typ: a.typ}
}

func (a *int8Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Int8MaxSels(vec.Col.([]int8), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Int8Max(vec.Col.([]int8))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *int8Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *int8Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(1)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		vs := []int8{0}
		copy(data[mempool.CountSize:], encoding.EncodeInt8Slice(vs))
		vec.Col = vs
	} else {
		vs := []int8{a.v}
		copy(data[mempool.CountSize:], encoding.EncodeInt8Slice(vs))
		vec.Col = vs
	}
	vec.Data = data
	return vec, nil
}
