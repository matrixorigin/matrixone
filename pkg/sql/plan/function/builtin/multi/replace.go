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

package multi

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Replace(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	thirdVector := vectors[2]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	thirdValues := vector.MustStrCols(thirdVector)
	resultType := types.T_varchar.ToType()

	if firstVector.IsScalarNull() || secondVector.IsScalarNull() || thirdVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	resultVector, err := proc.AllocVectorOfRows(resultType, 0, nil)
	if err != nil {
		return nil, err
	}

	if secondValues[0] == "" {
		vector.AppendString(resultVector, firstValues, proc.Mp())
	} else {
		sg := []string{strings.ReplaceAll(firstValues[0], secondValues[0], thirdValues[0])}
		vector.AppendString(resultVector, sg, proc.Mp())
	}

	return resultVector, nil
}
