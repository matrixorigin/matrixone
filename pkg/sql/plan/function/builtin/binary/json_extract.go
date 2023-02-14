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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type computeFn func([]byte, *bytejson.Path) (*bytejson.ByteJson, error)

func JsonExtract(vectors []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	jsonBytes, pathBytes := vectors[0], vectors[1]
	maxLen := jsonBytes.Length()
	if maxLen < pathBytes.Length() {
		maxLen = pathBytes.Length()
	}
	resultType := types.T_json.ToType()
	if jsonBytes.IsScalarNull() || pathBytes.IsScalarNull() {
		ret = proc.AllocConstNullVector(resultType, maxLen)
		return
	}

	var fn computeFn
	switch jsonBytes.Typ.Oid {
	case types.T_json:
		fn = types.ComputeJson
	default:
		fn = types.ComputeString
	}

	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	if jsonBytes.IsScalar() && pathBytes.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		resultValues := make([]*bytejson.ByteJson, 1)
		resultValues, err = json_extract.JsonExtract(json, path, []*nulls.Nulls{jsonBytes.Nsp, pathBytes.Nsp}, resultValues, ret.Nsp, fn)
		if err != nil {
			return
		}
		if ret.Nsp.Contains(0) {
			return
		}
		dt, _ := resultValues[0].Marshal()
		err = vector.SetBytesAt(ret, 0, dt, proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
	if err != nil {
		return
	}
	resultValues := make([]*bytejson.ByteJson, maxLen)
	resultValues, err = json_extract.JsonExtract(json, path, []*nulls.Nulls{jsonBytes.Nsp, pathBytes.Nsp}, resultValues, ret.Nsp, fn)
	if err != nil {
		return
	}
	for idx, v := range resultValues {
		if ret.Nsp.Contains(uint64(idx)) {
			continue
		}
		dt, _ := v.Marshal()
		err = vector.SetBytesAt(ret, idx, dt, proc.Mp())
		if err != nil {
			return
		}
	}
	return
}
