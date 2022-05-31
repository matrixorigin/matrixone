package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/year"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]types.Date)
	resultType := types.Type{Oid: types.T_uint16, Size: 2}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := make([]uint16, 1)
		vector.SetCol(resultVector, year.DateToYear(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeUint16Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, year.DateToYear(inputValues, resultValues))
		return resultVector, nil
	}
}

func DatetimeToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]types.Datetime)
	resultType := types.Type{Oid: types.T_uint16, Size: 2}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := make([]uint16, 1)
		vector.SetCol(resultVector, year.DatetimeToYear(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeUint16Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, year.DatetimeToYear(inputValues, resultValues))
		return resultVector, nil
	}
}
