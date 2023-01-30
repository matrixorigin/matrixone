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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type computeFn func([]byte, []*bytejson.Path) (*bytejson.ByteJson, error)

func computeJson(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(paths), nil
}
func computeString(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return nil, err
	}
	return bj.Query(paths), nil
}

func JsonExtract(vectors []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	jsonBytes := vectors[0]
	pathVecs := vectors[1:]
	maxLen := 0
	resultType := types.T_json.ToType()
	allScalar := true
	for _, v := range vectors {
		if v.Length() > maxLen {
			maxLen = v.Length()
		}
		if v.IsScalarNull() {
			ret = proc.AllocScalarNullVector(resultType)
			return
		}
		if !v.IsScalar() {
			allScalar = false
		}
	}

	var fn computeFn
	switch jsonBytes.Typ.Oid {
	case types.T_json:
		fn = computeJson
	default:
		fn = computeString
	}

	json := vector.MustBytesCols(jsonBytes)
	paths := make([][][]byte, len(pathVecs))
	nsps := make([]*nulls.Nulls, len(pathVecs)+1)
	nsps[0] = jsonBytes.Nsp
	for i, v := range pathVecs {
		paths[i] = vector.MustBytesCols(v)
		nsps[i+1] = v.Nsp
	}

	if allScalar {
		ret = proc.AllocScalarVector(resultType)
		resultValues := make([]*bytejson.ByteJson, 1)

		resultValues, err = json_extract.JsonExtract(json, paths, nsps, resultValues, ret.Nsp, 1, fn)
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
	resultValues, err = json_extract.JsonExtract(json, paths, nsps, resultValues, ret.Nsp, maxLen, fn)
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
