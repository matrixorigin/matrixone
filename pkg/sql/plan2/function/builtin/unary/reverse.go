// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/reverse"
	"github.com/matrixorigin/matrixone/pkg/vm/process2"
)

//reverse function's evaluation for arguments: [varchar] return type: varchar
func FdsReverseVarchar(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVecCol := vs[0].Col.(*types.Bytes)

	resultVec, err := process.Get(proc, int64(len(inputVecCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVec.Data,
		Offsets: make([]uint32, len(inputVecCol.Offsets)),
		Lengths: make([]uint32, len(inputVecCol.Lengths)),
	}
	nulls.Set(resultVec.Nsp, vs[0].Nsp)
	vector.SetCol(resultVec, reverse.ReverseVarChar(inputVecCol, results))
	return resultVec, nil
}

//reverse function's evaluation for arguments: [char] return type: char
func FdsReverseChar(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVecCol := vs[0].Col.(*types.Bytes)

	resultVec, err := process.Get(proc, int64(len(inputVecCol.Data)), types.Type{Oid: types.T_char, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVec.Data,
		Offsets: make([]uint32, len(inputVecCol.Offsets)),
		Lengths: make([]uint32, len(inputVecCol.Lengths)),
	}
	nulls.Set(resultVec.Nsp, vs[0].Nsp)
	vector.SetCol(resultVec, reverse.ReverseChar(inputVecCol, results))
	return resultVec, nil
}
