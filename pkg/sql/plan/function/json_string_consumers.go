// Copyright 2026 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const jsonStringConsumerOverloadID = 1

func jsonStringConsumerOverloadIndex(overloads []overload) int {
	for i, candidate := range overloads {
		if candidate.overloadId == jsonStringConsumerOverloadID {
			return i
		}
	}
	return -1
}

// getJSONStringConsumerValue preserves the stored byte representation for
// ordinary text and binary operands. JSON has a separate execution contract:
// only a non-SQL-NULL JSON value is serialized to its visible JSON bytes.
func getJSONStringConsumerValue(
	parameter vector.FunctionParameterWrapper[types.Varlena], row uint64,
) ([]byte, bool, error) {
	value, isNull := parameter.GetStrValue(row)
	if isNull || parameter.GetType().Oid != types.T_json {
		return value, isNull, nil
	}
	visible, err := types.DecodeJson(value).MarshalJSON()
	if err != nil {
		return nil, false, err
	}
	return visible, false, nil
}

func jsonStringConsumerReturnType(parameters []types.Type) types.Type {
	if hasBinaryStringDomain(parameters) {
		return types.T_blob.ToType()
	}
	return types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin)
}
