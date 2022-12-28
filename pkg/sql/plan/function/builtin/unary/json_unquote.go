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
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_unquote"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func JsonUnquote(vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	vec := vecs[0]
	var (
		fSingle func([]byte) (string, error)
		fBacth  func([][]byte, []string, *nulls.Nulls) ([]string, error)
	)
	switch {
	case types.IsString(vec.Typ.Oid):
		fSingle = json_unquote.StringSingle
		fBacth = json_unquote.StringBatch
	default:
		fSingle = json_unquote.JsonSingle
		fBacth = json_unquote.JsonBatch
	}
	resultType := types.T_varchar.ToType()
	if vec.IsScalarNull() {
		ret = proc.AllocScalarNullVector(resultType)
		return
	}
	if vec.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		v := vector.MustBytesCols(vec)[0]
		var r string
		r, err = fSingle(v)
		if err != nil {
			return nil, err
		}
		err = vector.SetStringAt(ret, 0, r, proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(vec.Length()), vec.Nsp)
	if err != nil {
		return nil, err
	}
	rs := vector.MustStrCols(vec)
	xs := vector.MustBytesCols(vec)
	rs, err = fBacth(xs, rs, ret.Nsp)
	if err != nil {
		return nil, err
	}
	for i, r := range rs {
		if ret.Nsp.Contains(uint64(i)) {
			continue
		}
		err = vector.SetStringAt(ret, i, r, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return
}
