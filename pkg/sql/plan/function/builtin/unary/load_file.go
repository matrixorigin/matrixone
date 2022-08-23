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
	"errors"
	"io/ioutil"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func LoadFile(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	vec := vector.New(resultType)
	const blobsize = 2 ^ 16 - 1
	fileName := vector.GetStrColumn(inputVector).GetString(0)
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	if len(content) > blobsize {
		return nil, errors.New("Data too long for blob")
	}
	if err := vec.Append(content, proc.GetMheap()); err != nil {
		return nil, err
	}
	return vec, nil
}
