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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/empty"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// empty function's evaluation for arguments: [varchar] and [char]
func FdsEmpty(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	col := vs[0].Col.(*types.Bytes)
	resultLen := len(col.Lengths)
	resultVec, err := process.Get(proc, int64(resultLen), types.Type{Oid: types.T_uint8, Size: 1})
	if err != nil {
		return nil, err
	}
	result := encoding.DecodeUint8Slice(resultVec.Data)
	result = result[:resultLen]
	resultVec.Col = result
	// the new vector's nulls are the same as the original vector
	nulls.Set(resultVec.Nsp, vs[0].Nsp)
	vector.SetCol(resultVec, empty.Empty(col, result))
	return resultVec, nil
}
