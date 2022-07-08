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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/length"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Length(vecs []vector.AnyVector, proc *process.Process) (vector.AnyVector, error) {
	rtyp := types.New(types.T_int64, 0, 0, 0)
	if vecs[0].IsScalarNull() {
		return proc.AllocScalarNullVector(rtyp), nil
	}
	ivec := vector.MustTVector[types.String](vecs[0])
	if ivec.IsScalar() {
		if ivec.IsScalarNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		rvec := vector.New[types.Int64](rtyp)
		rvec.IsConst = true
		rvec.Col = []types.Int64{types.Int64(len(ivec.Col[0]))}
		return rvec, nil
	}
	rvec := vector.New[types.Int64](rtyp)
	vs, err := rvec.ReallocForFixedSlice(len(ivec.Col), proc.Mp)
	if err != nil {
		return nil, err
	}
	vector.SetCol(rvec, length.StrLength(ivec.Col, vs))
	nulls.Set(rvec.Nsp, ivec.Nsp)
	return rvec, nil
}
