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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func JsonExtractByString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	jsonBytes, pathBytes := vectors[0], vectors[1]
	resultType := types.T_varchar.ToType()
	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	resultValues := make([][]byte, len(json))
	// XXX BUG: the function only handles path is a constant.
	_, err := json_extract.QueryByString(json, path, resultValues)
	if err != nil {
		logutil.Infof("json_extract: err:%v", err)
		return nil, err
	}
	// No null map?
	return vector.NewWithBytes(resultType, resultValues, nil, proc.Mp()), nil
}

func JsonExtractByJson(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	jsonBytes, pathBytes := vectors[0], vectors[1]
	resultType := types.T_varchar.ToType()
	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	resultValues := make([][]byte, len(json))
	// XXX BUG: the function only handles if path is constant.
	_, err := json_extract.QueryByJson(json, path, resultValues)
	if err != nil {
		logutil.Infof("json_extract: err:%v", err)
		return nil, err
	}
	return vector.NewWithBytes(resultType, resultValues, nil, proc.Mp()), nil
}
