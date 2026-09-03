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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const jsonMemberOfFunctionName = "member of"

// jsonMemberOfCheckFn keeps the left operand typed so SQL strings remain JSON
// strings, while the right operand follows the JSON document rules used by
// JSON_OVERLAPS.
func jsonMemberOfCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 2 || !jsonConstructorSupportsType(inputs[0].Oid) {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	finalTypes := []types.Type{inputs[0], inputs[1]}
	needsCast := false
	if inputs[0].Oid == types.T_any {
		// Prepared parameters arrive as T_any until execution.  Match the
		// JSON constructor/checker convention so a NULL or text parameter is
		// still handled by the normal SQL NULL path without inventing a JSON
		// parse for its value.
		finalTypes[0] = types.T_varchar.ToType()
		needsCast = true
	}
	if inputs[1].Oid != types.T_json && !inputs[1].Oid.IsMySQLString() {
		if inputs[1].Oid == types.T_any {
			finalTypes[1] = types.T_varchar.ToType()
			needsCast = true
		} else {
			return newCheckResultWithInvalidJSONArgument(2)
		}
	}
	if needsCast {
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithSuccess(0)
}

type jsonMemberOfValueOperand struct {
	parameter *vector.Vector
	cached    bool
	document  bytejson.ByteJson
	isNull    bool
	err       error
}

func (operand *jsonMemberOfValueOperand) documentAt(row uint64, proc *process.Process) (bytejson.ByteJson, bool, error) {
	if operand.parameter.IsConst() && operand.cached {
		return operand.document, operand.isNull, operand.err
	}

	if operand.parameter.IsNull(row) {
		operand.isNull = true
		if operand.parameter.IsConst() {
			operand.cached = true
		}
		return bytejson.Null, true, nil
	}

	elem, err := (&opBuiltInJsonArray{}).convertToAny(proc, operand.parameter, int(row))
	if err == nil {
		operand.document, err = bytejson.CreateByteJSON(elem)
		if err == nil && operand.parameter.GetType().Oid == types.T_json {
			err = bytejson.ValidateJSONDocumentDepth(operand.document)
		}
	}
	operand.isNull = false
	operand.err = err
	if operand.parameter.IsConst() {
		operand.cached = true
	}
	return operand.document, false, err
}

func jsonMemberOf(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	rs := vector.MustFunctionResult[int64](result)
	if selectList != nil && selectList.IgnoreAllRow() {
		for i := 0; i < length; i++ {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
		return nil
	}

	rs.UseOptFunctionParamFrame(2)
	left := jsonMemberOfValueOperand{parameter: parameters[0]}
	right := jsonOverlapOperand{
		parameter:    parameters[1],
		wrapper:      vector.OptGetBytesParamFromWrapper(rs, 1, parameters[1]),
		functionName: jsonMemberOfFunctionName,
	}
	defer right.prepared.clear()

	evaluableRows := 0
	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			continue
		}
		if parameters[0].IsNull(row) {
			continue
		}
		if _, isNull := right.wrapper.GetStrValue(row); !isNull {
			evaluableRows++
		}
	}

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		leftDocument, leftNull, err := left.documentAt(row, proc)
		if err != nil {
			return err
		}
		if leftNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		rightDocument, rightNull, err := right.documentAt(row, proc)
		if err != nil {
			return err
		}
		if rightNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		matched := false
		if rightDocument.Type == bytejson.TpCodeArray {
			if right.parameter.IsConst() && jsonOverlapShouldPrepareScalar(rightDocument.GetElemCnt(), evaluableRows) {
				right.prepared.ensure(rightDocument)
				matched = jsonOverlapPreparedArrayContains(rightDocument, &right.prepared, leftDocument)
			} else {
				matched = jsonOverlapArrayContains(rightDocument, leftDocument)
			}
		} else {
			matched = equalJSONOverlapExact(leftDocument, rightDocument)
		}
		value := int64(0)
		if matched {
			value = 1
		}
		if err := rs.Append(value, false); err != nil {
			return err
		}
	}
	return nil
}
