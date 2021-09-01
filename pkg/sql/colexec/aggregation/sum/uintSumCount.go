package sum

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/sum"
	"matrixone/pkg/vm/process"
)

func NewUintSumCount(typ types.Type) *uintSumCount {
	return &uintSumCount{typ: typ}
}

func (a *uintSumCount) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *uintSumCount) Type() types.Type {
	return a.typ
}

func (a *uintSumCount) Dup() aggregation.Aggregation {
	return &uintSumCount{typ: a.typ}
}

func (a *uintSumCount) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		switch vec.Typ.Oid {
		case types.T_uint8:
			a.sum += sum.Uint8SumSels(vec.Col.([]uint8), sels)
		case types.T_uint16:
			a.sum += sum.Uint16SumSels(vec.Col.([]uint16), sels)
		case types.T_uint32:
			a.sum += sum.Uint32SumSels(vec.Col.([]uint32), sels)
		case types.T_uint64:
			a.sum += sum.Uint64SumSels(vec.Col.([]uint64), sels)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		switch vec.Typ.Oid {
		case types.T_int8:
			a.sum += sum.Uint8Sum(vec.Col.([]uint8))
		case types.T_int16:
			a.sum += sum.Uint16Sum(vec.Col.([]uint16))
		case types.T_int32:
			a.sum += sum.Uint32Sum(vec.Col.([]uint32))
		case types.T_int64:
			a.sum += sum.Uint64Sum(vec.Col.([]uint64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *uintSumCount) Eval() interface{} {
	return []interface{}{a.cnt, float64(a.sum)}
}

func (a *uintSumCount) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(a.typ)
	vec.SetCol([][]interface{}{[]interface{}{a.cnt, float64(a.sum)}})
	return vec, nil
}
