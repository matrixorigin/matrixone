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

//Min:        3,
//Max:        3,
//Typ:        ty
//Typ:        types.T_varchar,
//ReturnType: types.T_varchar,
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

	lens := vecs[1].Col.([]int64)
	padds := vecs[2].Col.(*types.Bytes)
	for _, num := range lens {
		if num < 0 {
			vec, err := proc.AllocVector(types.Type{Oid: types.T_varchar, Size: 24}, 24*int64(len(vs.Lengths)))
			if vecs[0].IsScalar() {
				vec.IsConst = true
			}

			if err != nil {
				return nil, err
			}
			nulls.Set(vec.Nsp, vecs[0].Nsp)
			temp := ""
			lengths_temp := []uint32{}
			offsets_temp := []uint32{}
			for k := 0; k < len(vs.Lengths); k++ {
				temp += "NULL"
				lengths_temp = append(lengths_temp, 4)
				if len(offsets_temp) == 0 {
					offsets_temp = append(offsets_temp, 0)
				} else {
					offsets_temp = append(offsets_temp, offsets_temp[len(offsets_temp)-1]+4)
				}

			}
			res := &types.Bytes{
				Data:    []byte(temp),
				Lengths: lengths_temp,
				Offsets: offsets_temp,
			}
			vector.SetCol(vec, res)
			return vec, nil
		}
	}

	vec, err := proc.AllocVector(types.Type{Oid: types.T_varchar, Size: 24}, 24*int64(len(vs.Lengths)))
	if vecs[0].IsScalar() {
		vec.IsConst = true
	}
	if err != nil {
		return nil, err
	}
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, lpad.LpadVarchar(vs, lens, padds))
	return vec, nil
}
