package count

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func New(typ types.Type) *count {
	return &count{typ: typ}
}

func (a *count) Reset() {
	a.cnt = 0
}

func (a *count) Type() types.Type {
	return a.typ
}

func (a *count) Dup() aggregation.Aggregation {
	return &count{typ: a.typ}
}

func (a *count) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *count) Eval() interface{} {
	return a.cnt
}

func (a *count) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	copy(data[mempool.CountSize:], encoding.EncodeInt64(a.cnt))
	vec.Data = data
	vec.Col = encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+8])
	return vec, nil
}
