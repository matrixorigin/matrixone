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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	JsonMemberOfFunctionName = "member of"
	jsonMemberOfFunctionName = JsonMemberOfFunctionName
)

func preparedJSONMemberOfScalarValue(
	ctx context.Context,
	value []byte,
	kind vector.PrepareParamKind,
	paramType types.T,
	binaryString bool,
	protocolVersion int64,
) (any, error) {
	switch paramType {
	case types.T_bit:
		// COM_STMT_EXECUTE does not carry BIT(n)'s width, so a uint64 JSON
		// number would silently lose the opaque byte/width contract.
		return nil, moerr.NewNotSupportedf(
			ctx, "prepared BIT parameters are not supported by MEMBER OF without width metadata")
	case types.T_enum:
		// ENUM values arriving through the prepared transport are already their
		// display labels. Preserve the label even if a transport-side binary bit
		// is present; the SQL domain is more specific than that transport flag.
		return string(value), nil
	case types.T_geometry, types.T_geometry32:
		geoJSON, err := geometryToGeoJSONBytes(value)
		if err != nil {
			return nil, err
		}
		return types.ParseSliceToByteJson(geoJSON)
	case types.T_uuid:
		return string(value), nil
	default:
		return PreparedJSONScalarValue(
			ctx, value, kind, paramType, binaryString, protocolVersion)
	}
}

// MEMBER OF accepts SQL scalar values and JSON documents.  Native vector
// values are SQL arrays, not JSON arrays, and MySQL rejects them as operands
// instead of implicitly converting them to a JSON document.
func jsonMemberOfLeftSupportsType(oid types.T) bool {
	if oid == types.T_geometry32 {
		return true
	}
	return jsonConstructorSupportsType(oid) && !oid.IsArrayRelate()
}

func jsonMemberOfRightHasBinaryDomain(typ types.Type) bool {
	switch typ.Oid {
	case types.T_binary, types.T_varbinary, types.T_blob:
		return true
	case types.T_char, types.T_varchar, types.T_text:
		return types.StaticStringDomain(typ) == types.StringDomainBinary
	default:
		return false
	}
}

// The right operand is a JSON document, not an arbitrary MySQL string.  In
// particular, MySQL rejects BINARY/VARBINARY/BLOB and text values in the
// binary charset before attempting to parse their bytes as JSON.
func jsonMemberOfRightSupportsType(typ types.Type) bool {
	if typ.Oid == types.T_json || typ.Oid == types.T_any {
		return true
	}
	if typ.Oid != types.T_char && typ.Oid != types.T_varchar && typ.Oid != types.T_text {
		return false
	}
	return !jsonMemberOfRightHasBinaryDomain(typ)
}

func jsonMemberOfRightIsBinary(parameter *vector.Vector, row int) bool {
	if parameter == nil || parameter.IsNull(uint64(row)) {
		return false
	}
	typ := *parameter.GetType()
	if jsonMemberOfRightHasBinaryDomain(typ) {
		return true
	}
	preparedType := parameter.GetPrepareParamType()
	switch preparedType {
	case types.T_binary, types.T_varbinary, types.T_blob:
		return true
	}
	return parameter.GetIsBinaryStringAt(row)
}

// jsonMemberOfRightHasInvalidRuntimeType preserves the SQL domain of a direct
// prepared RHS. The parameter vector uses text as its transport type, so the
// concrete type and conversion kind must be checked before the bytes can be
// interpreted as JSON text. A missing runtime type remains the generic
// prepared-plan case and is handled by the existing scalar conversion path.
func jsonMemberOfRightHasInvalidRuntimeType(parameter *vector.Vector, row int) bool {
	if parameter == nil || parameter.IsNull(uint64(row)) {
		return false
	}
	runtimeType := parameter.GetType().Oid
	if runtimeType != types.T_any && runtimeType != types.T_json && !runtimeType.IsMySQLString() {
		return true
	}
	preparedType := parameter.GetPrepareParamType()
	if preparedType != types.T_any {
		return !jsonMemberOfRightSupportsType(preparedType.ToType())
	}
	return parameter.GetPrepareParamKindAt(row) != vector.PrepareParamNone
}

// jsonMemberOfLeftHasInvalidRuntimeType rejects a direct prepared value using
// its concrete SQL domain before the text transport can erase that domain.
// This is especially important for vector values: formatting []float32 as
// text would otherwise make an SQL array look like a JSON scalar.
func jsonMemberOfLeftHasInvalidRuntimeType(parameter *vector.Vector, row int) bool {
	if parameter == nil || parameter.IsNull(uint64(row)) {
		return false
	}
	if preparedType := parameter.GetPrepareParamType(); preparedType != types.T_any {
		// The binary protocol does not carry BIT(n)'s declared width. Fail closed
		// instead of comparing a width-dependent opaque value as uint64 JSON.
		if preparedType == types.T_bit {
			return true
		}
		return !jsonMemberOfLeftSupportsType(preparedType)
	}
	return !jsonMemberOfLeftSupportsType(parameter.GetType().Oid)
}

func jsonMemberOfInvalidType(proc *process.Process, argument int) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	return moerr.NewInvalidTypeForJSON(ctx, argument, jsonMemberOfFunctionName)
}

func jsonMemberOfInvalidRightType(proc *process.Process, binaryCharset bool) error {
	if binaryCharset {
		ctx := context.Background()
		if proc != nil && proc.Ctx != nil {
			ctx = proc.Ctx
		}
		return moerr.NewInvalidJSONCharset(ctx, "binary")
	}
	return jsonMemberOfInvalidType(proc, 2)
}

// jsonMemberOfCheckFn keeps the left operand typed so SQL strings remain JSON
// strings, while the right operand follows the JSON document rules used by
// JSON_OVERLAPS.
func jsonMemberOfCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 2 || !jsonMemberOfLeftSupportsType(inputs[0].Oid) {
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
	if inputs[1].Oid == types.T_any {
		finalTypes[1] = types.T_varchar.ToType()
		needsCast = true
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
	if operand.parameter.GetType().Oid == types.T_enum {
		return bytejson.Null, false, jsonMemberOfInvalidType(proc, 1)
	}

	var (
		elem any
		err  error
	)
	if operand.parameter.GetType().Oid.IsMySQLString() {
		ctx := context.Background()
		if proc != nil && proc.Ctx != nil {
			ctx = proc.Ctx
		}
		kind := operand.parameter.GetPrepareParamKindAt(int(row))
		paramType := operand.parameter.GetPrepareParamType()
		if paramType == types.T_any && kind == vector.PrepareParamNone {
			// A non-prepared binary/text vector has no prepared metadata. Use
			// its concrete OID so static BINARY/VARBINARY/BLOB values retain
			// their opaque JSON domain, while prepared text transport with a
			// numeric/Boolean kind still follows that kind below.
			paramType = operand.parameter.GetType().Oid
		}
		elem, err = preparedJSONMemberOfScalarValue(
			ctx,
			operand.parameter.GetBytesAt(int(row)),
			kind,
			paramType,
			operand.parameter.GetIsBinaryStringAt(int(row)),
			jsonSessionProtocolVersion(proc),
		)
	} else if operand.parameter.GetType().Oid == types.T_year {
		// YEAR is stored as a temporal SQL type, but MEMBER OF compares its
		// scalar value in the JSON number domain. Keep this local to MEMBER OF;
		// JSON_ARRAY/JSON_SET intentionally retain their existing temporal
		// string formatting contract.
		elem = uint64(vector.GetFixedAtNoTypeCheck[types.MoYear](operand.parameter, int(row)))
	} else if operand.parameter.GetType().Oid == types.T_geometry ||
		operand.parameter.GetType().Oid == types.T_geometry32 {
		geoJSON, geoErr := geometryToGeoJSONBytes(operand.parameter.GetBytesAt(int(row)))
		if geoErr == nil {
			elem, geoErr = types.ParseSliceToByteJson(geoJSON)
		}
		err = geoErr
	} else {
		elem, err = (&opBuiltInJsonArray{}).convertToAny(proc, operand.parameter, int(row), jsonSessionProtocolVersion(proc))
	}
	if err == nil {
		operand.document, err = bytejson.CreateByteJSON(elem)
		if err == nil && (operand.parameter.GetType().Oid == types.T_json ||
			operand.parameter.GetPrepareParamType() == types.T_json) {
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
		functionName: jsonMemberOfFunctionName,
	}
	// Static non-string RHS domains are rejected at execution time so a SQL
	// NULL left operand can short-circuit them. Do not build a varlena wrapper
	// for fixed-width vectors; GenerateFunctionStrParameter assumes varlena
	// storage and would inspect the invalid representation before execution.
	if parameters[1].GetType().Oid.IsMySQLString() || parameters[1].GetType().Oid == types.T_json {
		right.wrapper = vector.OptGetBytesParamFromWrapper(rs, 1, parameters[1])
	}
	defer right.prepared.clear()

	evaluableRows := 0
	usePreparedArray := false
	preparedArrayDecisionReady := false

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// SQL NULL on either side short-circuits MEMBER OF before validating
		// prepared domains or parsing the other operand. In particular, the
		// prepared BIT/vector limitation must not turn `? MEMBER OF (NULL)`
		// into an argument-1 type error.
		if parameters[0].IsNull(row) || parameters[1].IsNull(row) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		if jsonMemberOfLeftHasInvalidRuntimeType(parameters[0], int(row)) {
			return jsonMemberOfInvalidType(proc, 1)
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
		if jsonMemberOfRightIsBinary(parameters[1], int(row)) {
			return jsonMemberOfInvalidRightType(proc, true)
		}
		if jsonMemberOfRightHasInvalidRuntimeType(parameters[1], int(row)) {
			return jsonMemberOfInvalidRightType(proc, false)
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
		if !preparedArrayDecisionReady && right.parameter.IsConst() &&
			rightDocument.Type == bytejson.TpCodeArray &&
			rightDocument.GetElemCnt() > jsonOverlapLinearCompareBudget {
			evaluableRows = jsonMemberOfCountEvaluableRows(parameters[0], length, selectList)
			usePreparedArray = jsonOverlapShouldPrepareScalar(rightDocument.GetElemCnt(), evaluableRows)
			preparedArrayDecisionReady = true
		}

		matched := false
		if rightDocument.Type == bytejson.TpCodeArray {
			if right.parameter.IsConst() && usePreparedArray {
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

// jsonMemberOfCountEvaluableRows is intentionally called only after the
// constant RHS has been decoded and proven large enough to consider indexing.
// Scalar, small-array, and non-constant RHS executions therefore retain one
// evaluation pass instead of paying an unconditional batch scan.
func jsonMemberOfCountEvaluableRows(
	left *vector.Vector,
	length int,
	selectList *FunctionSelectList,
) int {
	count := 0
	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			continue
		}
		if !left.IsNull(row) {
			count++
		}
	}
	return count
}
