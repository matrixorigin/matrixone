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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lpad"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const UINT16_MAX = ^uint16(0)

func Lpad(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(vecs[0].Typ), nil
	}
	vs := vecs[0].Col.(*types.Bytes) //Get the first arg
	if !vecs[1].IsScalar() && vecs[1].Typ.Oid != types.T_int64 {
		return nil, errors.New("The second argument of the lpad function must be an int64 constant")
	}
	if !vecs[2].IsScalar() && vecs[2].Typ.Oid != types.T_varchar {
		return nil, errors.New("The third argument of the lpad function must be an string constant")
	}

	length := vecs[1].Col.([]int64)
	padds := vecs[2].Col.(*types.Bytes)

	if vecs[0].IsScalar() {
		if length[0] < 0 || length[0] > int64(UINT16_MAX) {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_varchar, Size: 24}), nil
		} else {
			resultVec := proc.AllocScalarVector(types.Type{Oid: types.T_varchar, Size: 24})
			results := &types.Bytes{
				Data:    make([]byte, length[0]),
				Offsets: make([]uint32, 1),
				Lengths: make([]uint32, 1),
			}
			nulls.Set(resultVec.Nsp, vecs[0].Nsp)
			vector.SetCol(resultVec, lpad.Lpad(results, vs, uint32(length[0]), padds))
			return resultVec, nil
		}
	} else {
		if length[0] < 0 || length[0] > int64(UINT16_MAX) {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_varchar, Size: 24}), nil
		} else {
			resultVec, err := proc.AllocVector(types.Type{Oid: types.T_varchar, Size: 24}, int64(len(vs.Lengths))*length[0])
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(vs.Offsets)),
				Lengths: make([]uint32, len(vs.Lengths)),
			}
			nulls.Set(resultVec.Nsp, vecs[0].Nsp)
			vector.SetCol(resultVec, lpad.Lpad(results, vs, uint32(length[0]), padds))
			return resultVec, nil
		}
	}
}
