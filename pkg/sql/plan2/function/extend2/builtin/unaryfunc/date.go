package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
Typ:        types.T_date,
ReturnType: types.T_date,
Fn: func(lv *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
	lvs := lv.Col.([]types.Date)
	size := types.T(types.T_date).TypeLen()
	if lv.Ref == 1 || lv.Ref == 0 {
		lv.Ref = 0
		date.DateToDate(lvs, lvs)
		return lv, nil
	}
	vec, err := process.Get(proc, int64(size)*int64(len(lvs)), types.Type{Oid: types.T_date, Size: int32(size)})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeDateSlice(vec.Data)
	rs = rs[:len(lvs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, lv.Nsp)
	vector.SetCol(vec, date.DateToDate(lvs, rs))
	return vec, nil

}

*/
func DateToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]types.Date)
	resultType := types.Type{Oid: types.T_date, Size: 4}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := make([]types.Date, 1)
		copy(resultValues, inputValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDateSlice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		copy(resultValues, inputValues)
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}

func DatetimeToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]types.Datetime)
	resultType := types.Type{Oid: types.T_date, Size: 4}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := make([]types.Date, 1)
		vector.SetCol(resultVector, date.DatetimeToDate(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDateSlice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, date.DatetimeToDate(inputValues, resultValues))
		return resultVector, nil
	}
}
