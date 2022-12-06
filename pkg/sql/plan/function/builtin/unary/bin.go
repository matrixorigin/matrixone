// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/bin"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Bin[T constraints.Unsigned | constraints.Signed](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalBin[T](vectors, proc, bin.Bin[T])
}

func BinFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalBin[T](vectors, proc, bin.BinFloat[T])
}

type binT interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

type binFun[T binT] func(*vector.Vector, *vector.Vector, *process.Process) error

func generalBin[T binT](vectors []*vector.Vector, proc *process.Process, cb binFun[T]) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		resultValues := make([]types.Varlena, 0, 1)
		vector.SetCol(resultVector, resultValues)
		err := cb(inputVector, resultVector, proc)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("The input value is out of range")
		}
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, 0, inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		err = cb(inputVector, resultVector, proc)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("The input value is out of range")
		}
		return resultVector, nil
	}

}
