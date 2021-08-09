package sum

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vm/process"
)

func NewSumCount(typ types.Type) *sumCount {
	return &sumCount{typ: typ}
}

func (a *sumCount) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *sumCount) Type() types.Type {
	return a.typ
}

func (a *sumCount) Dup() aggregation.Aggregation {
	return &sumCount{typ: a.typ}
}

func (a *sumCount) Fill(sels []int64, vec *vector.Vector) error {
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

func (a *sumCount) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return []interface{}{a.cnt, a.sum}
}

func (a *sumCount) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(a.typ)
	vec.SetCol([][]interface{}{[]interface{}{a.cnt, a.sum}})
	return vec, nil
}
