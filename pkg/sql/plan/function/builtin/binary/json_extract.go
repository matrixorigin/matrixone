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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func JsonExtractByString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var (
		err    error
		length int
		ret    *vector.Vector
	)
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	jsonBytes, pathBytes := vectors[0], vectors[1]
	resultType := types.T_varchar
	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	if jsonBytes.IsScalar() && pathBytes.IsScalar() {
		resultValues := make([]*bytejson.ByteJson, 0, 1)
		resultValues, err = json_extract.QueryByString(json, path, resultValues)
		if err != nil {
			return nil, err
		}
		ret = vector.New(types.Type{Oid: resultType})
		for _, v := range resultValues {
			err = ret.Append([]byte(v.String()), v.IsNull(), proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		ret.MakeScalar(1)
		return ret, nil
	}
	if len(json) > len(path) {
		length = len(json)
	} else {
		length = len(path)
	}
	resultValues := make([]*bytejson.ByteJson, 0, length)
	resultValues, err = json_extract.QueryByString(json, path, resultValues)
	if err != nil {
		return nil, err
	}
	ret = vector.New(types.Type{Oid: resultType})
	for _, v := range resultValues {
		err = ret.Append([]byte(v.String()), v.IsNull(), proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func JsonExtractByJson(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var (
		err    error
		length int
		ret    *vector.Vector
	)
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	jsonBytes, pathBytes := vectors[0], vectors[1]
	resultType := types.T_varchar
	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	if jsonBytes.IsScalar() && pathBytes.IsScalar() {
		resultValues := make([]*bytejson.ByteJson, 0, 1)
		resultValues, err = json_extract.QueryByJson(json, path, resultValues)
		if err != nil {
			return nil, err
		}
		ret = vector.New(types.Type{Oid: resultType})
		for _, v := range resultValues {
			err = ret.Append([]byte(v.String()), v.IsNull(), proc.Mp())
			if err != nil {
				return nil, err
			}
		}
		ret.MakeScalar(1)
		return ret, nil
	}
	if len(json) > len(path) {
		length = len(json)
	} else {
		length = len(path)
	}
	resultValues := make([]*bytejson.ByteJson, 0, length)
	resultValues, err = json_extract.QueryByJson(json, path, resultValues)
	if err != nil {
		return nil, err
	}
	ret = vector.New(types.Type{Oid: resultType})
	for _, v := range resultValues {
		err = ret.Append([]byte(v.String()), v.IsNull(), proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
