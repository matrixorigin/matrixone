package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/space"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func SpaceInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]int64)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		results := &types.Bytes{
			Data:    make([]byte, inputValues[0]),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		vector.SetCol(resultVector, space.FillSpacesInt64(inputValues, results))
		return resultVector, nil
	}
	bytesNeed := space.CountSpacesForInt64(inputValues)
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	nulls.Set(resultVector.Nsp, inputVector.Nsp)
	vector.SetCol(resultVector, space.FillSpacesInt64(inputValues, resultValues))
	return resultVector, nil

}

func SpaceUint64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]uint64)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		results := &types.Bytes{
			Data:    make([]byte, inputValues[0]),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		vector.SetCol(resultVector, space.FillSpacesUint64(inputValues, results))
		return resultVector, nil
	}
	bytesNeed := space.CountSpacesForUint64(inputValues)
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	nulls.Set(resultVector.Nsp, inputVector.Nsp)
	vector.SetCol(resultVector, space.FillSpacesUint64(inputValues, resultValues))
	return resultVector, nil

}

func SpaceFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.([]T)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		results := &types.Bytes{
			Data:    make([]byte, inputValues[0]),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		vector.SetCol(resultVector, space.FillSpacesFloat[T](inputValues, results))
		return resultVector, nil
	}
	bytesNeed := space.CountSpacesForFloat[T](inputValues)
	resultVector, err := proc.AllocVector(resultType, bytesNeed)
	if err != nil {
		return nil, err
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(inputValues)),
		Lengths: make([]uint32, len(inputValues)),
	}
	nulls.Set(resultVector.Nsp, inputVector.Nsp)
	vector.SetCol(resultVector, space.FillSpacesFloat[T](inputValues, resultValues))
	return resultVector, nil

}
