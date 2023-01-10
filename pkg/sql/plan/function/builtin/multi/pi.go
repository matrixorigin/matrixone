// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Pi(_ []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultVector := vector.New(vector.CONSTANT, resultType)
	result := make([]float64, 1)
	result[0] = pi.GetPi()
	vector.SetCol(resultVector, result)
	return resultVector, nil
}
