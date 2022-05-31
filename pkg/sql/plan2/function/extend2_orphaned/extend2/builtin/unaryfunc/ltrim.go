package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ltrim"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Ltrim(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.(*types.Bytes)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}

	// totalCount - spaceCount is the total bytes need for the ltrim-ed string
	spaceCount := ltrim.CountSpacesFromLeft(inputValues)
	totalCount := int32(len(inputValues.Data))
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := &types.Bytes{
			Data:    make([]byte, totalCount-spaceCount),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		vector.SetCol(resultVector, ltrim.LtrimChar(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(totalCount-spaceCount))
		if err != nil {
			return nil, err
		}
		resultValues := &types.Bytes{
			Data:    resultVector.Data,
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, ltrim.LtrimChar(inputValues, resultValues))
		return resultVector, nil
	}
}
