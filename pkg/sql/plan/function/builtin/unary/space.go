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
	"github.com/matrixorigin/matrixone/pkg/vectorize/space"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func SpaceNumber[T types.BuiltinNumber](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustTCols[T](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return proc.AllocConstNullVector(resultType), nil
		}
		results := make([]string, 1)
		_, err := space.FillSpacesNumber(inputValues, results)
		if err != nil {
			return nil, err
		}
		vec := vector.New(vector.CONSTANT, resultType)
		vector.AppendString(vec, results[0], false, proc.Mp())
		return vec, nil
	}

	results := make([]string, len(inputValues))
	if _, err := space.FillSpacesNumber(inputValues, results); err != nil {
		return nil, err
	}
	vec := vector.New(vector.FLAT, resultType)
	vector.AppendStringList(vec, results, nil, proc.Mp())
	return vec, nil
}
