package avg

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/process"
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
	vs := encoding.DecodeFloat64Slice(data[:8])
	if a.cnt == 0 {
		vs[0] = 0
		vec.Nsp.Add(0)
	} else {
		vs[0] = float64(a.sum) / float64(a.cnt)
	}
	vec.Col = vs
	vec.Data = data
	return vec, nil
}
