package avg

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/sum"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewUint(typ types.Type) *uintAvg {
	return &uintAvg{typ: typ}
}

func (a *uintAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *uintAvg) Type() types.Type {
	return a.typ
}

func (a *uintAvg) Dup() aggregation.Aggregation {
	return &uintAvg{typ: a.typ}
}

func (a *uintAvg) Fill(sels []int64, vec *vector.Vector) error {
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

func (a *uintAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return float64(a.sum) / float64(a.cnt)
}

func (a *uintAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(float64(a.sum)/float64(a.cnt)))
	}
	vec.Data = data
	vec.Col = encoding.DecodeFloat64Slice(data[mempool.CountSize : mempool.CountSize+8])
	return vec, nil
}
