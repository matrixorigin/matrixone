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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Values(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fromVec := parameters[0]
	toVec := result.GetResultVector()

	sels := make([]int32, fromVec.Length())
	for j := 0; j < len(sels); j++ {
		sels[j] = int32(j)
	}
	toVec.Union(fromVec, sels, proc.GetMPool())
	return nil
}
