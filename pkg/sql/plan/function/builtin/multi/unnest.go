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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/unnest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Unnest(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	jsonVec := inputVecs[0]
	pathVec := inputVecs[1]
	OuterVec := inputVecs[2]
	json, path := vector.MustBytesCols(jsonVec), vector.MustBytesCols(pathVec)
	outer := vector.MustTCols[bool](OuterVec)
	resultType := types.T_batch.ToType()
	resultVector := vector.New(resultType)
	attrs := []string{"seq", "key", "path", "index", "value", "this"}
	bat := batch.New(false, attrs)
	bat, err := unnest.Unnest(json, path, outer, bat)
	if err != nil {
		return nil, err
	}
	vector.SetCol(resultVector, bat)
	return resultVector, nil
}
