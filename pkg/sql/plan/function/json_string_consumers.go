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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	visible, err := concatStringValue(parameter.GetType().Oid, value)
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

func jsonStringConsumerResultParameters(parameters []types.Type) []types.Type {
	// JSON visible text has no VARCHAR width bound. Preserve that fact when
	// deriving output metadata as well as when consuming the runtime value.
	if len(parameters) == 0 || parameters[0].Oid != types.T_json {
		return parameters
	}
	result := append([]types.Type(nil), parameters...)
	result[0] = jsonStringConsumerReturnType(parameters[:1])
	return result
}

// Keep JSON in its source type so each consumer can hash or transform the
// complete visible JSON bytes without an implicit VARCHAR width limit.
func jsonStringConsumerUnaryFixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return jsonStringConsumerTypeMatch(overloads, inputs, fixedTypeMatch)
}

func jsonStringConsumerUnaryDomainTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return jsonStringConsumerTypeMatch(overloads, inputs, stringDomainFixedTypeMatch)
}

func jsonStringConsumerTypeMatch(
	overloads []overload,
	inputs []types.Type,
	match func([]overload, []types.Type) checkResult,
) checkResult {
	if len(inputs) == 0 || inputs[0].Oid != types.T_json {
		return match(overloads, inputs)
	}
	prepared := append([]types.Type(nil), inputs...)
	prepared[0] = types.T_varchar.ToType()
	matched := match(overloads, prepared)
	if matched.status == succeedMatched {
		return newCheckResultWithSuccess(matched.idx)
	}
	if matched.status != succeedWithCast || len(matched.finalType) != len(inputs) {
		return matched
	}
	castTypes := append([]types.Type(nil), matched.finalType...)
	castTypes[0] = inputs[0]
	for i := range inputs {
		if !inputs[i].Eq(castTypes[i]) {
			return newCheckResultWithCast(matched.idx, castTypes)
		}
	}
	return newCheckResultWithSuccess(matched.idx)
}

func jsonStringConsumerUnaryBytesToBytes(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	resultFn func([]byte) []byte,
	selectList *FunctionSelectList,
) error {
	if parameters[0].GetType().Oid != types.T_json {
		return opUnaryBytesToBytes(parameters, result, proc, length, resultFn, selectList)
	}
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		value, isNull, err := getJSONStringConsumerValue(source, row)
		if err != nil {
			return err
		}
		if isNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendBytes(resultFn(value), false); err != nil {
			return err
		}
	}
	return nil
}
