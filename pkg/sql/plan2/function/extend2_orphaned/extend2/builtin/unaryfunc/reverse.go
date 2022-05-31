package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/reverse"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Reverse(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	inputValues := inputVector.Col.(*types.Bytes)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	// resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := &types.Bytes{
			Data:    make([]byte, len(inputValues.Data)),
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		vector.SetCol(resultVector, reverse.ReverseChar(inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(len(inputValues.Data)))
		if err != nil {
			return nil, err
		}
		resultValues := &types.Bytes{
			Data:    resultVector.Data,
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, reverse.ReverseChar(inputValues, resultValues))
		return resultVector, nil
	}
}
