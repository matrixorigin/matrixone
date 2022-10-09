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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/reverse"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Reverse(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustStrCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, 1)
		reverse.Reverse(inputValues, resultValues)
		return vector.NewConstString(resultType, inputVector.Length(), resultValues[0], proc.Mp()), nil
	} else {
		resultValues := make([]string, len(inputValues))
		reverse.Reverse(inputValues, resultValues)
		return vector.NewWithStrings(resultType, resultValues, inputVector.Nsp, proc.Mp()), nil
	}
}
