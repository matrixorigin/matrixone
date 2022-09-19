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


import(
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/matrixorigin/matrixone/pkg/vectorize/bin"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
	"fmt"
)

func Bin[T constraints.Unsigned | constraints.Signed](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, 1)
		bin.Bin(inputValues, resultValues)
		return vector.NewConstString(resultType, inputVector.Length(), resultValues[0]), nil
	} else {
		resultValues := make([]string, len(inputValues))
		bin.Bin(inputValues, resultValues)
		return vector.NewWithStrings(resultType, resultValues, inputVector.Nsp, proc.Mp()), nil
	}
}


func BinFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, 1)
		_, err := bin.BinFloat(inputValues, resultValues)
		if err != nil{
			return nil, fmt.Errorf("The input value is out of range")
		}
		return vector.NewConstString(resultType, inputVector.Length(), resultValues[0]), nil
	} else {
		resultValues := make([]string, len(inputValues))
		_, err := bin.BinFloat(inputValues, resultValues)
		if err != nil{
			return nil, fmt.Errorf("The input value is out of range")
		}
		return vector.NewWithStrings(resultType, resultValues, inputVector.Nsp, proc.Mp()), nil
	}
}