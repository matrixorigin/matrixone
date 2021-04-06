package max

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vectorize/max"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func NewInt64(typ types.Type) *int64Max {
	return &int64Max{typ: typ}
}

func (a *int64Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *int64Max) Type() types.Type {
	return a.typ
}

func (a *int64Max) Dup() aggregation.Aggregation {
	return &int64Max{typ: a.typ}
}

func (a *int64Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Int64MaxSels(vec.Col.([]int64), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Int64Max(vec.Col.([]int64))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *int64Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *int64Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeInt64(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeInt64(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+8])
	return vec, nil
}
