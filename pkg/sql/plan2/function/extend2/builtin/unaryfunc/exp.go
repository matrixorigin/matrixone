package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/exp"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sin"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Exp[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0] // so these kinds of indexing are always guaranteed success because all the checks are done in the plan?
	inputValues := inputVector.Col.([]T)
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType)
		resultValues := make([]float64, 1)
		vector.SetCol(resultVector, sin.Sin[T](inputValues, resultValues))
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, exp.Exp[T](inputValues, resultValues))
		return resultVector, nil
	}
}
