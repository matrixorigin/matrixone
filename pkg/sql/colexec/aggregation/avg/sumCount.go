package avg

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/process"
)

func NewSumCount(typ types.Type) *sumCountAvg {
	return &sumCountAvg{typ: typ}
}

func (a *sumCountAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *sumCountAvg) Type() types.Type {
	return a.typ
}

func (a *sumCountAvg) Dup() aggregation.Aggregation {
	return &sumCountAvg{typ: a.typ}
}

func (a *sumCountAvg) Fill(sels []int64, vec *vector.Vector) error {
	vs := vec.Col.([][]interface{})
	if n := len(sels); n > 0 {
		for _, sel := range sels {
			a.cnt += vs[sel][0].(int64)
			a.sum += vs[sel][1].(float64)
		}
	} else {
		for _, v := range vs {
			a.cnt += v[0].(int64)
			a.sum += v[1].(float64)
		}
	}
	return nil
}

func (a *sumCountAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.sum / float64(a.cnt)
}

func (a *sumCountAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(a.sum/float64(a.cnt)))
	}
	vec.Data = data
	vec.Col = encoding.DecodeFloat64Slice(data[mempool.CountSize : mempool.CountSize+8])
	return vec, nil
}
