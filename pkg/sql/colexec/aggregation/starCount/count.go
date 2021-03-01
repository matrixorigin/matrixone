package starCount

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func New() *count {
	return &count{}
}

func (a *count) Reset() {
	a.cnt = 0
}

func (a *count) Dup() aggregation.Aggregation {
	return &count{}
}

func (a *count) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		a.cnt += int64(n)
	} else {
		a.cnt += int64(vec.Length())
	}
	return nil
}

func (a *count) Eval(proc *process.Process) (*vector.Vector, error) {
	data := proc.Mp.Alloc(8)
	vec := vector.New(types.Type{types.T_int64, 8, 8, 0})
	copy(data[mempool.CountSize:], encoding.EncodeInt64(a.cnt))
	vec.Data = data
	vec.Col = encoding.DecodeInt64Slice(data[mempool.CountSize:])
	return vec, nil
}
