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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type computeFn func([]byte, *bytejson.Path) (*bytejson.ByteJson, error)

func computeJson(json []byte, path *bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(path), nil
}
func computeString(json []byte, path *bytejson.Path) (*bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return nil, err
	}
	return bj.Query(path), nil
}

func JsonExtract(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	jsonBytes, pathBytes := vectors[0], vectors[1]
	maxLen := jsonBytes.Length()
	if maxLen < pathBytes.Length() {
		maxLen = pathBytes.Length()
	}
	rtyp := types.T_json.ToType()
	if jsonBytes.IsConstNull() || pathBytes.IsConstNull() {
		return vector.NewConstNull(rtyp, jsonBytes.Length(), proc.Mp()), nil
	}

	var fn computeFn
	switch jsonBytes.GetType().Oid {
	case types.T_json:
		fn = computeJson
	default:
		fn = computeString
	}

	if jsonBytes.IsConst() && pathBytes.IsConst() {
		rval, err := jsonExtract(jsonBytes.GetBytes(0), pathBytes.GetBytes(0), fn)
		if err != nil {
			return nil, err
		}
		dt, _ := rval.Marshal()
		return vector.NewConstBytes(rtyp, dt, jsonBytes.Length(), proc.Mp()), err
	}
	rvec, err := proc.AllocVectorOfRows(rtyp, maxLen, nil)
	nulls.Or(jsonBytes.GetNulls(), pathBytes.GetNulls(), rvec.GetNulls())
	if err != nil {
		return nil, err
	}
	for i := 0; i < jsonBytes.Length(); i++ {
		if rvec.GetNulls().Contains(uint64(i)) {
			continue
		}
		rval, err := jsonExtract(jsonBytes.GetBytes(i), pathBytes.GetBytes(i), fn)
		if err != nil {
			return nil, err
		}
		dt, _ := rval.Marshal()
		err = vector.SetBytesAt(rvec, i, dt, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return rvec, nil
}

func jsonExtract(json, path []byte, compute func([]byte, *bytejson.Path) (*bytejson.ByteJson, error)) (*bytejson.ByteJson, error) {
	pStar, err := types.ParseStringToPath(string(path))
	if err != nil {
		return nil, err
	}
	return compute(json, &pStar)
}
