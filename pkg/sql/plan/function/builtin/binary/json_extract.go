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

func JsonExtract(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	jsonBytes, pathBytes := vectors[0], vectors[1]
	logutil.Infof("JsonExtract: jsonBytes=%s, pathBytes=%s,typeJ:%T,typeP:%T", jsonBytes, pathBytes, jsonBytes.Col, pathBytes.Col)
	resultType := types.Type{Oid: types.T_varchar, Size: 30}
	json, path := vector.MustBytesCols(jsonBytes), vector.MustBytesCols(pathBytes)
	resultElementSize := int(resultType.Size)
	resultVector, err := proc.AllocVector(resultType, int64((resultElementSize)*len(json.Lengths)))
	if err != nil {
		return nil, err
	}
	resultValues := resultVector.Col.(*types.Bytes)
	//change string to types.Bytes
	//outBytes := &types.Bytes{
	//	Data:    make([]byte, 10),
	//	Lengths: []uint32{10},
	//	Offsets: []uint32{0},
	//}
	//outBytes.Data = []byte("hello,json")
	vector.SetCol(resultVector, json_extract.JsonExtract(json, path, resultValues))
	logutil.Infof("JsonExtract: resultVector=%s,type:%T", resultVector, resultVector.Col)
	return resultVector, nil
}
