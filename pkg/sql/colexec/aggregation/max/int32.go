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

func NewInt32(typ types.Type) *int32Max {
	return &int32Max{typ: typ}
}

func (a *int32Max) Reset() {
	a.v = 0
	a.cnt = 0
}

func (a *int32Max) Type() types.Type {
	return a.typ
}

func (a *int32Max) Dup() aggregation.Aggregation {
	return &int32Max{typ: a.typ}
}

func (a *int32Max) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		v := max.Int32MaxSels(vec.Col.([]int32), sels)
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		v := max.Int32Max(vec.Col.([]int32))
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
		if a.cnt == 0 || v > a.v {
			a.v = v
		}
	}
	return nil
}

func (a *int32Max) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.v
}

func (a *int32Max) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(4)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeInt32(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeInt32(a.v))
	}
	vec.Data = data
	vec.Col = encoding.DecodeInt32Slice(data[mempool.CountSize : mempool.CountSize+4])
	return vec, nil
}
