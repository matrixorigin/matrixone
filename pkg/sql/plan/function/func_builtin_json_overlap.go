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
	"bytes"
	"math"
	"math/big"
	"math/bits"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const jsonOverlapLinearCompareBudget = 256

// Every Decimal256 value has an absolute value smaller than 1e77. Keep
// overflow double values out of Parse256: its generic string parser is not a
// safe conversion path for exponents that require more than Decimal256's
// representable precision.
const jsonOverlapDecimal256OverflowMagnitude = 1e77

type jsonOverlapWorkspace struct {
	indexes []uint32
}

type jsonOverlapPreparedArray struct {
	indexes     []uint32
	numericKeys map[uint32]*jsonOverlapPreparedNumericKey
	ready       bool
}

type jsonOverlapNumericKeyState uint8

const (
	jsonOverlapNumericKeyUninitialized jsonOverlapNumericKeyState = iota
	jsonOverlapNumericKeyValid
	jsonOverlapNumericKeyInvalid
)

type jsonOverlapPreparedNumericKey struct {
	state jsonOverlapNumericKeyState
	key   jsonOverlapNumericKey
}

type jsonOverlapValueView struct {
	document bytejson.ByteJson
	prepared *jsonOverlapPreparedArray
}

type jsonOverlapOperand struct {
	parameter *vector.Vector
	wrapper   vector.FunctionParameterWrapper[types.Varlena]
	cached    bool
	document  bytejson.ByteJson
	isNull    bool
	err       error
	prepared  jsonOverlapPreparedArray
}

func jsonOverlapsCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	finalTypes := make([]types.Type, 2)
	needsCast := false
	for i, input := range inputs {
		switch input.Oid {
		case types.T_json, types.T_char, types.T_varchar, types.T_text:
			finalTypes[i] = input
		case types.T_any:
			finalTypes[i] = types.T_varchar.ToType()
			needsCast = true
		default:
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	if needsCast {
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithSuccess(0)
}

func jsonOverlaps(
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
	left := jsonOverlapOperand{
		parameter: parameters[0],
		wrapper:   vector.OptGetBytesParamFromWrapper(rs, 0, parameters[0]),
	}
	right := jsonOverlapOperand{
		parameter: parameters[1],
		wrapper:   vector.OptGetBytesParamFromWrapper(rs, 1, parameters[1]),
	}
	var workspace jsonOverlapWorkspace
	defer workspace.clear()
	defer left.prepared.clear()
	defer right.prepared.clear()
	evaluableRows := jsonOverlapEvaluableRows(&left, &right, length, selectList)

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

		value := int64(0)
		if workspace.overlapsViews(left.valueView(leftDocument), right.valueView(rightDocument), evaluableRows) {
			value = 1
		}
		if err := rs.Append(value, false); err != nil {
			return err
		}
	}
	return nil
}

func jsonOverlapEvaluableRows(left, right *jsonOverlapOperand, length int, selectList *FunctionSelectList) int {
	count := 0
	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			continue
		}
		_, leftNull := left.wrapper.GetStrValue(row)
		if leftNull {
			continue
		}
		_, rightNull := right.wrapper.GetStrValue(row)
		if !rightNull {
			count++
		}
	}
	return count
}

func (operand *jsonOverlapOperand) documentAt(
	row uint64,
	proc *process.Process,
) (bytejson.ByteJson, bool, error) {
	if operand.parameter.IsConst() && operand.cached {
		return operand.document, operand.isNull, operand.err
	}

	data, isNull := operand.wrapper.GetStrValue(row)
	var document bytejson.ByteJson
	var err error
	if !isNull {
		if operand.parameter.GetType().Oid == types.T_json {
			document = types.DecodeJson(data)
			err = bytejson.ValidateJSONDocumentDepth(document)
		} else {
			document, err = types.ParseSliceToByteJsonWithDepthLimit(
				data,
				bytejson.JSONDocumentMaxNestingDepth,
			)
		}
		if err != nil && !bytejson.IsJSONDocumentDepthError(err) {
			err = moerr.NewInvalidArg(proc.Ctx, "json_overlaps", "invalid JSON document")
		}
	}

	if operand.parameter.IsConst() {
		operand.cached = true
		operand.document = document
		operand.isNull = isNull
		operand.err = err
	}
	return document, isNull, err
}

func (operand *jsonOverlapOperand) valueView(document bytejson.ByteJson) jsonOverlapValueView {
	view := jsonOverlapValueView{document: document}
	if operand.parameter.IsConst() && document.Type == bytejson.TpCodeArray {
		view.prepared = &operand.prepared
	}
	return view
}

func (w *jsonOverlapWorkspace) clear() {
	w.indexes = nil
}

func (prepared *jsonOverlapPreparedArray) clear() {
	prepared.indexes = nil
	prepared.numericKeys = nil
	prepared.ready = false
}

func (prepared *jsonOverlapPreparedArray) ensure(array bytejson.ByteJson) {
	if prepared.ready {
		return
	}
	count := array.GetElemCnt()
	prepared.indexes = make([]uint32, count)
	for index := range prepared.indexes {
		prepared.indexes[index] = uint32(index)
	}
	slices.SortFunc(prepared.indexes, func(a, b uint32) int {
		return prepared.compareElements(array, a, b)
	})
	prepared.ready = true
}

func (prepared *jsonOverlapPreparedArray) numericKey(index uint32) *jsonOverlapPreparedNumericKey {
	if prepared.numericKeys == nil {
		prepared.numericKeys = make(map[uint32]*jsonOverlapPreparedNumericKey)
	}
	key := prepared.numericKeys[index]
	if key == nil {
		key = &jsonOverlapPreparedNumericKey{}
		prepared.numericKeys[index] = key
	}
	return key
}

func (prepared *jsonOverlapPreparedArray) compareElements(array bytejson.ByteJson, leftIndex, rightIndex uint32) int {
	left := array.GetArrayElem(int(leftIndex))
	right := array.GetArrayElem(int(rightIndex))
	if !jsonOverlapNumericType(left.Type) || !jsonOverlapNumericType(right.Type) {
		return compareJSONOverlapExact(left, right)
	}
	leftDecimal, rightDecimal, fast := jsonOverlapNumericFastValues(left, right)
	if fast {
		return compareJSONOverlapNumericFast(left, right, leftDecimal, rightDecimal)
	}
	return compareJSONOverlapNumericKeys(left, right, prepared.numericKey(leftIndex), prepared.numericKey(rightIndex))
}

func (prepared *jsonOverlapPreparedArray) compareElementWithValue(
	array bytejson.ByteJson,
	arrayIndex uint32,
	value bytejson.ByteJson,
	valueKey *jsonOverlapPreparedNumericKey,
) int {
	arrayValue := array.GetArrayElem(int(arrayIndex))
	if !jsonOverlapNumericType(arrayValue.Type) || !jsonOverlapNumericType(value.Type) {
		return compareJSONOverlapExact(arrayValue, value)
	}
	arrayDecimal, valueDecimal, fast := jsonOverlapNumericFastValues(arrayValue, value)
	if fast {
		return compareJSONOverlapNumericFast(arrayValue, value, arrayDecimal, valueDecimal)
	}
	return compareJSONOverlapNumericKeys(arrayValue, value, prepared.numericKey(arrayIndex), valueKey)
}

func (w *jsonOverlapWorkspace) overlaps(left, right bytejson.ByteJson) bool {
	return w.overlapsViews(jsonOverlapValueView{document: left}, jsonOverlapValueView{document: right}, 1)
}

func (w *jsonOverlapWorkspace) overlapsViews(left, right jsonOverlapValueView, evaluableRows int) bool {
	if left.document.Type != bytejson.TpCodeArray && right.document.Type == bytejson.TpCodeArray {
		left, right = right, left
	}

	switch left.document.Type {
	case bytejson.TpCodeArray:
		if right.document.Type == bytejson.TpCodeArray {
			return w.arraysOverlapViews(left, right)
		}
		if left.prepared != nil && jsonOverlapShouldPrepareScalar(left.document.GetElemCnt(), evaluableRows) {
			left.prepared.ensure(left.document)
			return jsonOverlapPreparedArrayContains(left.document, left.prepared, right.document)
		}
		return jsonOverlapArrayContains(left.document, right.document)
	case bytejson.TpCodeObject:
		return right.document.Type == bytejson.TpCodeObject && jsonOverlapObjects(left.document, right.document)
	default:
		return equalJSONOverlapExact(left.document, right.document)
	}
}

func (w *jsonOverlapWorkspace) arraysOverlapViews(left, right jsonOverlapValueView) bool {
	if left.document.GetElemCnt() > right.document.GetElemCnt() {
		left, right = right, left
	}
	if left.document.GetElemCnt() == 0 || right.document.GetElemCnt() == 0 {
		return false
	}
	if left.document.GetElemCnt() <= jsonOverlapLinearCompareBudget/right.document.GetElemCnt() {
		return w.arraysOverlap(left.document, right.document)
	}
	if left.prepared != nil {
		left.prepared.ensure(left.document)
		return jsonOverlapPreparedArrayOverlaps(left.document, left.prepared, right.document)
	}
	return w.arraysOverlap(left.document, right.document)
}

func jsonOverlapShouldPrepareScalar(count, evaluableRows int) bool {
	if count <= jsonOverlapLinearCompareBudget || evaluableRows <= 1 {
		return false
	}
	logCount := bits.Len(uint(count))
	return int64(evaluableRows)*int64(count) > int64(count)*int64(logCount)+int64(evaluableRows)*int64(logCount)
}

func jsonOverlapPreparedArrayOverlaps(array bytejson.ByteJson, prepared *jsonOverlapPreparedArray, probe bytejson.ByteJson) bool {
	for index := 0; index < probe.GetElemCnt(); index++ {
		if jsonOverlapPreparedArrayContains(array, prepared, probe.GetArrayElem(index)) {
			return true
		}
	}
	return false
}

func jsonOverlapPreparedArrayContains(array bytejson.ByteJson, prepared *jsonOverlapPreparedArray, value bytejson.ByteJson) bool {
	var probeKey jsonOverlapPreparedNumericKey
	position := sort.Search(len(prepared.indexes), func(index int) bool {
		arrayIndex := prepared.indexes[index]
		return prepared.compareElementWithValue(array, arrayIndex, value, &probeKey) >= 0
	})
	if position == len(prepared.indexes) {
		return false
	}
	arrayIndex := prepared.indexes[position]
	return prepared.compareElementWithValue(array, arrayIndex, value, &probeKey) == 0
}

func (w *jsonOverlapWorkspace) arraysOverlap(left, right bytejson.ByteJson) bool {
	leftCount := left.GetElemCnt()
	rightCount := right.GetElemCnt()
	if leftCount == 0 || rightCount == 0 {
		return false
	}
	if leftCount > rightCount {
		left, right = right, left
		leftCount, rightCount = rightCount, leftCount
	}

	if leftCount <= jsonOverlapLinearCompareBudget/rightCount {
		for i := 0; i < leftCount; i++ {
			value := left.GetArrayElem(i)
			for j := 0; j < rightCount; j++ {
				if equalJSONOverlapExact(value, right.GetArrayElem(j)) {
					return true
				}
			}
		}
		return false
	}

	w.prepareIndexes(leftCount)
	slices.SortFunc(w.indexes, func(a, b uint32) int {
		return compareJSONOverlapExact(left.GetArrayElem(int(a)), left.GetArrayElem(int(b)))
	})
	for i := 0; i < rightCount; i++ {
		value := right.GetArrayElem(i)
		position := sort.Search(len(w.indexes), func(j int) bool {
			return compareJSONOverlapExact(left.GetArrayElem(int(w.indexes[j])), value) >= 0
		})
		if position < len(w.indexes) &&
			equalJSONOverlapExact(left.GetArrayElem(int(w.indexes[position])), value) {
			return true
		}
	}
	return false
}

func (w *jsonOverlapWorkspace) prepareIndexes(count int) {
	if cap(w.indexes) < count {
		w.indexes = make([]uint32, count)
	} else {
		w.indexes = w.indexes[:count]
	}
	for i := range w.indexes {
		w.indexes[i] = uint32(i)
	}
}

func jsonOverlapArrayContains(array, value bytejson.ByteJson) bool {
	for i := 0; i < array.GetElemCnt(); i++ {
		if equalJSONOverlapExact(array.GetArrayElem(i), value) {
			return true
		}
	}
	return false
}

func jsonOverlapObjects(left, right bytejson.ByteJson) bool {
	leftIndex, rightIndex := 0, 0
	for leftIndex < left.GetElemCnt() && rightIndex < right.GetElemCnt() {
		keyCompare := bytes.Compare(left.GetObjectKey(leftIndex), right.GetObjectKey(rightIndex))
		switch {
		case keyCompare < 0:
			leftIndex++
		case keyCompare > 0:
			rightIndex++
		default:
			if equalJSONOverlapExact(left.GetObjectVal(leftIndex), right.GetObjectVal(rightIndex)) {
				return true
			}
			leftIndex++
			rightIndex++
		}
	}
	return false
}

func equalJSONOverlapExact(left, right bytejson.ByteJson) bool {
	return compareJSONOverlapExact(left, right) == 0
}

func compareJSONOverlapExact(left, right bytejson.ByteJson) int {
	if jsonOverlapNumericType(left.Type) && jsonOverlapNumericType(right.Type) {
		return compareJSONOverlapNumeric(left, right)
	}
	leftRank := jsonOverlapTypeRank(left.Type)
	rightRank := jsonOverlapTypeRank(right.Type)
	if leftRank != rightRank {
		return compareInt(leftRank, rightRank)
	}
	if left.Type != right.Type {
		return compareInt(int(left.Type), int(right.Type))
	}

	switch left.Type {
	case bytejson.TpCodeLiteral:
		return compareInt(int(left.Data[0]), int(right.Data[0]))
	case bytejson.TpCodeString, bytejson.TpCodeBlob:
		return bytes.Compare(left.GetString(), right.GetString())
	case bytejson.TpCodeDate:
		return compareJSONOverlapDate(left, right)
	case bytejson.TpCodeTime:
		return compareJSONOverlapTime(left, right)
	case bytejson.TpCodeDatetime:
		return compareJSONOverlapDatetime(left, right)
	case bytejson.TpCodeArray:
		return compareJSONOverlapArrays(left, right)
	case bytejson.TpCodeObject:
		return compareJSONOverlapObjects(left, right)
	default:
		return bytes.Compare(left.Data, right.Data)
	}
}

func compareJSONOverlapArrays(left, right bytejson.ByteJson) int {
	leftCount := left.GetElemCnt()
	rightCount := right.GetElemCnt()
	count := min(leftCount, rightCount)
	for i := 0; i < count; i++ {
		if result := compareJSONOverlapExact(left.GetArrayElem(i), right.GetArrayElem(i)); result != 0 {
			return result
		}
	}
	return compareInt(leftCount, rightCount)
}

func compareJSONOverlapObjects(left, right bytejson.ByteJson) int {
	leftCount := left.GetElemCnt()
	rightCount := right.GetElemCnt()
	if result := compareInt(leftCount, rightCount); result != 0 {
		return result
	}
	for i := 0; i < leftCount; i++ {
		if result := bytes.Compare(left.GetObjectKey(i), right.GetObjectKey(i)); result != 0 {
			return result
		}
		if result := compareJSONOverlapExact(left.GetObjectVal(i), right.GetObjectVal(i)); result != 0 {
			return result
		}
	}
	return 0
}

func jsonOverlapNumericType(valueType bytejson.TpCode) bool {
	switch valueType {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return true
	default:
		return false
	}
}

func jsonOverlapTypeRank(valueType bytejson.TpCode) int {
	switch valueType {
	case bytejson.TpCodeLiteral:
		return 0
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return 1
	case bytejson.TpCodeString:
		return 2
	case bytejson.TpCodeObject:
		return 3
	case bytejson.TpCodeArray:
		return 4
	case bytejson.TpCodeDate:
		return 5
	case bytejson.TpCodeTime:
		return 6
	case bytejson.TpCodeDatetime:
		return 7
	case bytejson.TpCodeBlob:
		return 8
	default:
		return 9 + int(valueType)
	}
}

func compareJSONOverlapNumeric(left, right bytejson.ByteJson) int {
	return compareJSONOverlapNumericWithKeys(left, right, nil, nil)
}

func compareJSONOverlapNumericWithKeys(
	left, right bytejson.ByteJson,
	leftKey, rightKey *jsonOverlapPreparedNumericKey,
) int {
	leftDecimal, rightDecimal, fast := jsonOverlapNumericFastValues(left, right)
	if !fast {
		return compareJSONOverlapNumericKeys(left, right, leftKey, rightKey)
	}
	return compareJSONOverlapNumericFast(left, right, leftDecimal, rightDecimal)
}

func jsonOverlapNumericFastValues(
	left, right bytejson.ByteJson,
) (jsonOverlapDecimalValue, jsonOverlapDecimalValue, bool) {
	if left.Type != bytejson.TpCodeDecimal && right.Type != bytejson.TpCodeDecimal {
		return jsonOverlapDecimalValue{}, jsonOverlapDecimalValue{}, true
	}
	leftDecimal, leftFast := jsonOverlapParseDecimalFast(left)
	rightDecimal, rightFast := jsonOverlapParseDecimalFast(right)
	if !leftFast || !rightFast || !jsonOverlapCanFastAlign(left, right, leftDecimal, rightDecimal) {
		return jsonOverlapDecimalValue{}, jsonOverlapDecimalValue{}, false
	}
	return leftDecimal, rightDecimal, true
}

func compareJSONOverlapNumericFast(
	left, right bytejson.ByteJson,
	leftDecimal, rightDecimal jsonOverlapDecimalValue,
) int {
	switch left.Type {
	case bytejson.TpCodeInt64:
		switch right.Type {
		case bytejson.TpCodeInt64:
			return compareInt64(left.GetInt64(), right.GetInt64())
		case bytejson.TpCodeUint64:
			if left.GetInt64() < 0 {
				return -1
			}
			return compareUint64(uint64(left.GetInt64()), right.GetUint64())
		case bytejson.TpCodeFloat64:
			return -compareJSONOverlapFloat64Int64(right.GetFloat64(), left.GetInt64())
		case bytejson.TpCodeDecimal:
			return compareJSONOverlapDecimalValues(jsonOverlapDecimalFromInt64(left.GetInt64()), rightDecimal)
		}
	case bytejson.TpCodeUint64:
		switch right.Type {
		case bytejson.TpCodeInt64:
			if right.GetInt64() < 0 {
				return 1
			}
			return compareUint64(left.GetUint64(), uint64(right.GetInt64()))
		case bytejson.TpCodeUint64:
			return compareUint64(left.GetUint64(), right.GetUint64())
		case bytejson.TpCodeFloat64:
			return -compareJSONOverlapFloat64Uint64(right.GetFloat64(), left.GetUint64())
		case bytejson.TpCodeDecimal:
			return compareJSONOverlapDecimalValues(jsonOverlapDecimalFromUint64(left.GetUint64()), rightDecimal)
		}
	case bytejson.TpCodeFloat64:
		switch right.Type {
		case bytejson.TpCodeInt64:
			return compareJSONOverlapFloat64Int64(left.GetFloat64(), right.GetInt64())
		case bytejson.TpCodeUint64:
			return compareJSONOverlapFloat64Uint64(left.GetFloat64(), right.GetUint64())
		case bytejson.TpCodeFloat64:
			return compareFloat64Total(left.GetFloat64(), right.GetFloat64())
		case bytejson.TpCodeDecimal:
			return compareJSONOverlapFloat64Decimal(left.GetFloat64(), rightDecimal)
		}
	case bytejson.TpCodeDecimal:
		switch right.Type {
		case bytejson.TpCodeInt64:
			return compareJSONOverlapDecimalValues(leftDecimal, jsonOverlapDecimalFromInt64(right.GetInt64()))
		case bytejson.TpCodeUint64:
			return compareJSONOverlapDecimalValues(leftDecimal, jsonOverlapDecimalFromUint64(right.GetUint64()))
		case bytejson.TpCodeFloat64:
			return -compareJSONOverlapFloat64Decimal(right.GetFloat64(), leftDecimal)
		case bytejson.TpCodeDecimal:
			return compareJSONOverlapDecimalValues(leftDecimal, rightDecimal)
		}
	default:
		return 0
	}
	return 0
}

func jsonOverlapParseDecimalFast(value bytejson.ByteJson) (jsonOverlapDecimalValue, bool) {
	if value.Type != bytejson.TpCodeDecimal {
		return jsonOverlapDecimalValue{}, true
	}
	if !jsonOverlapDecimalFitsFastPath(value.GetString()) {
		return jsonOverlapDecimalValue{}, false
	}
	parsed, scale, err := types.Parse256(string(value.GetString()))
	if err != nil {
		return jsonOverlapDecimalValue{}, false
	}
	return jsonOverlapDecimalValue{value: parsed, scale: scale}, true
}

func jsonOverlapDecimalFitsFastPath(text []byte) bool {
	digits := 0
	for _, ch := range text {
		if ch == 'e' || ch == 'E' {
			break
		}
		if ch >= '0' && ch <= '9' {
			digits++
		}
	}
	return digits <= 76
}

func jsonOverlapCanFastAlign(left, right bytejson.ByteJson, leftDecimal, rightDecimal jsonOverlapDecimalValue) bool {
	if left.Type == bytejson.TpCodeFloat64 || right.Type == bytejson.TpCodeFloat64 {
		return true
	}
	leftValue := leftDecimal
	if left.Type == bytejson.TpCodeInt64 {
		leftValue = jsonOverlapDecimalFromInt64(left.GetInt64())
	} else if left.Type == bytejson.TpCodeUint64 {
		leftValue = jsonOverlapDecimalFromUint64(left.GetUint64())
	}
	rightValue := rightDecimal
	if right.Type == bytejson.TpCodeInt64 {
		rightValue = jsonOverlapDecimalFromInt64(right.GetInt64())
	} else if right.Type == bytejson.TpCodeUint64 {
		rightValue = jsonOverlapDecimalFromUint64(right.GetUint64())
	}
	if !leftValue.value.Sign() && !rightValue.value.Sign() &&
		leftValue.value == (types.Decimal256{}) && rightValue.value == (types.Decimal256{}) {
		return true
	}
	delta := int64(leftValue.scale) - int64(rightValue.scale)
	if delta < 0 {
		delta = -delta
	}
	if delta > 76 {
		return false
	}
	if leftValue.scale < rightValue.scale {
		_, err := leftValue.value.Scale(rightValue.scale - leftValue.scale)
		return err == nil
	}
	if leftValue.scale > rightValue.scale {
		_, err := rightValue.value.Scale(leftValue.scale - rightValue.scale)
		return err == nil
	}
	return true
}

type jsonOverlapNumericKey struct {
	sign     int8
	digits   string
	exponent big.Int
	adjusted big.Int
}

func compareJSONOverlapNumericKeys(
	left, right bytejson.ByteJson,
	leftCache, rightCache *jsonOverlapPreparedNumericKey,
) int {
	if result, handled := compareJSONOverlapNonFiniteFloat64(left, right); handled {
		return result
	}
	leftKey, leftOK := jsonOverlapNumericKeyFromByteJSONCached(left, leftCache)
	rightKey, rightOK := jsonOverlapNumericKeyFromByteJSONCached(right, rightCache)
	if !leftOK || !rightOK {
		if leftOK != rightOK {
			if leftOK {
				return -1
			}
			return 1
		}
		if result := compareInt(int(left.Type), int(right.Type)); result != 0 {
			return result
		}
		return bytes.Compare(left.Data, right.Data)
	}
	return compareJSONOverlapNumericKey(leftKey, rightKey)
}

func compareJSONOverlapNonFiniteFloat64(left, right bytejson.ByteJson) (int, bool) {
	leftFloat, leftNonFinite := jsonOverlapNonFiniteFloat64(left)
	rightFloat, rightNonFinite := jsonOverlapNonFiniteFloat64(right)
	if !leftNonFinite && !rightNonFinite {
		return 0, false
	}
	if leftNonFinite && rightNonFinite {
		return compareFloat64Total(leftFloat, rightFloat), true
	}
	if leftNonFinite {
		if math.IsInf(leftFloat, -1) {
			return -1, true
		}
		return 1, true
	}
	if math.IsInf(rightFloat, -1) {
		return 1, true
	}
	return -1, true
}

func jsonOverlapNonFiniteFloat64(value bytejson.ByteJson) (float64, bool) {
	if value.Type != bytejson.TpCodeFloat64 {
		return 0, false
	}
	floatValue := value.GetFloat64()
	return floatValue, math.IsNaN(floatValue) || math.IsInf(floatValue, 0)
}

func jsonOverlapNumericKeyFromByteJSONCached(
	value bytejson.ByteJson,
	cache *jsonOverlapPreparedNumericKey,
) (jsonOverlapNumericKey, bool) {
	if cache != nil {
		switch cache.state {
		case jsonOverlapNumericKeyValid:
			return cache.key, true
		case jsonOverlapNumericKeyInvalid:
			return jsonOverlapNumericKey{}, false
		}
	}
	key, ok := jsonOverlapNumericKeyFromByteJSON(value)
	if cache != nil {
		if ok {
			cache.state = jsonOverlapNumericKeyValid
			cache.key = key
		} else {
			cache.state = jsonOverlapNumericKeyInvalid
		}
	}
	return key, ok
}

func jsonOverlapNumericKeyFromByteJSON(value bytejson.ByteJson) (jsonOverlapNumericKey, bool) {
	switch value.Type {
	case bytejson.TpCodeInt64:
		return jsonOverlapParseNumericKey(strconv.FormatInt(value.GetInt64(), 10))
	case bytejson.TpCodeUint64:
		return jsonOverlapParseNumericKey(strconv.FormatUint(value.GetUint64(), 10))
	case bytejson.TpCodeFloat64:
		floatValue := value.GetFloat64()
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return jsonOverlapNumericKey{}, false
		}
		return jsonOverlapParseNumericKey(strconv.FormatFloat(floatValue, 'g', -1, 64))
	case bytejson.TpCodeDecimal:
		return jsonOverlapParseNumericKey(string(value.GetString()))
	default:
		return jsonOverlapNumericKey{}, false
	}
}

func jsonOverlapParseNumericKey(text string) (jsonOverlapNumericKey, bool) {
	var key jsonOverlapNumericKey
	if text == "" {
		return key, false
	}
	index := 0
	if text[index] == '-' {
		key.sign = -1
		index++
	} else {
		key.sign = 1
	}
	if index == len(text) {
		return jsonOverlapNumericKey{}, false
	}
	digits := make([]byte, 0, len(text))
	fractionDigits := 0
	hasDot := false
	for index < len(text) && text[index] != 'e' && text[index] != 'E' {
		ch := text[index]
		switch {
		case ch >= '0' && ch <= '9':
			digits = append(digits, ch)
			if hasDot {
				fractionDigits++
			}
		case ch == '.' && !hasDot:
			hasDot = true
		default:
			return jsonOverlapNumericKey{}, false
		}
		index++
	}
	if len(digits) == 0 {
		return jsonOverlapNumericKey{}, false
	}
	exponentText := "0"
	if index < len(text) {
		index++
		if index == len(text) {
			return jsonOverlapNumericKey{}, false
		}
		exponentText = text[index:]
	}
	if _, ok := key.exponent.SetString(exponentText, 10); !ok {
		return jsonOverlapNumericKey{}, false
	}
	first := 0
	for first < len(digits) && digits[first] == '0' {
		first++
	}
	if first == len(digits) {
		key.sign = 0
		key.digits = "0"
		return key, true
	}
	last := len(digits) - 1
	for last >= first && digits[last] == '0' {
		last--
	}
	trailingZeros := len(digits) - 1 - last
	key.digits = string(digits[first : last+1])
	key.exponent.Add(&key.exponent, big.NewInt(int64(trailingZeros-fractionDigits)))
	key.adjusted.Set(&key.exponent)
	key.adjusted.Add(&key.adjusted, big.NewInt(int64(len(key.digits)-1)))
	return key, true
}

func compareJSONOverlapNumericKey(left, right jsonOverlapNumericKey) int {
	if left.sign != right.sign {
		return compareInt(int(left.sign), int(right.sign))
	}
	if left.sign == 0 {
		return 0
	}
	result := left.adjusted.Cmp(&right.adjusted)
	if result == 0 {
		length := max(len(left.digits), len(right.digits))
		for index := 0; index < length; index++ {
			leftDigit, rightDigit := byte('0'), byte('0')
			if index < len(left.digits) {
				leftDigit = left.digits[index]
			}
			if index < len(right.digits) {
				rightDigit = right.digits[index]
			}
			if leftDigit != rightDigit {
				result = compareInt(int(leftDigit), int(rightDigit))
				break
			}
		}
	}
	if left.sign < 0 {
		return -result
	}
	return result
}

func compareJSONOverlapFloat64Int64(left float64, right int64) int {
	rightFloat := float64(right)
	if left < rightFloat {
		return -1
	}
	if left > rightFloat {
		return 1
	}
	return compareJSONOverlapFloat64Decimal(left, jsonOverlapDecimalFromInt64(right))
}

func compareJSONOverlapFloat64Uint64(left float64, right uint64) int {
	if left < 0 {
		return -1
	}
	rightFloat := float64(right)
	if left < rightFloat {
		return -1
	}
	if left > rightFloat {
		return 1
	}
	return compareJSONOverlapFloat64Decimal(left, jsonOverlapDecimalFromUint64(right))
}

type jsonOverlapDecimalValue struct {
	value types.Decimal256
	scale int32
}

func jsonOverlapDecimalFromInt64(value int64) jsonOverlapDecimalValue {
	return jsonOverlapDecimalValue{value: types.Decimal256FromInt64(value)}
}

func jsonOverlapDecimalFromUint64(value uint64) jsonOverlapDecimalValue {
	return jsonOverlapDecimalValue{value: types.Decimal256{B0_63: value}}
}

func compareJSONOverlapFloat64Decimal(left float64, right jsonOverlapDecimalValue) int {
	if math.IsNaN(left) {
		return 1
	}
	if math.IsInf(left, 1) {
		return 1
	}
	if math.IsInf(left, -1) {
		return -1
	}
	if math.Abs(left) >= jsonOverlapDecimal256OverflowMagnitude {
		if left < 0 {
			return -1
		}
		return 1
	}
	format := byte('f')
	if math.Abs(left) < 1 {
		format = 'g'
	}
	text := strconv.FormatFloat(left, format, -1, 64)
	value, scale, err := types.Parse256(text)
	if err == nil {
		return compareJSONOverlapDecimalValues(jsonOverlapDecimalValue{value: value, scale: scale}, right)
	}
	if left < 0 {
		return -1
	}
	return 1
}

func compareJSONOverlapDecimalValues(left, right jsonOverlapDecimalValue) int {
	if left.scale < right.scale {
		scaled, err := left.value.Scale(right.scale - left.scale)
		if err != nil {
			if left.value.Sign() {
				return -1
			}
			return 1
		}
		left.value = scaled
	} else if left.scale > right.scale {
		scaled, err := right.value.Scale(left.scale - right.scale)
		if err != nil {
			if right.value.Sign() {
				return 1
			}
			return -1
		}
		right.value = scaled
	}
	return types.CompareDecimal256(left.value, right.value)
}

func compareJSONOverlapDate(left, right bytejson.ByteJson) int {
	leftValue, leftErr := types.ParseDateCast(string(left.GetString()))
	rightValue, rightErr := types.ParseDateCast(string(right.GetString()))
	if leftErr != nil || rightErr != nil {
		return bytes.Compare(left.GetString(), right.GetString())
	}
	return compareInt64(int64(leftValue), int64(rightValue))
}

func compareJSONOverlapTime(left, right bytejson.ByteJson) int {
	leftText := string(left.GetString())
	rightText := string(right.GetString())
	leftValue, leftErr := types.ParseTime(leftText, jsonOverlapTemporalScale(leftText))
	rightValue, rightErr := types.ParseTime(rightText, jsonOverlapTemporalScale(rightText))
	if leftErr != nil || rightErr != nil {
		return strings.Compare(leftText, rightText)
	}
	return compareInt64(int64(leftValue), int64(rightValue))
}

func compareJSONOverlapDatetime(left, right bytejson.ByteJson) int {
	leftText := string(left.GetString())
	rightText := string(right.GetString())
	leftValue, leftErr := types.ParseDatetime(leftText, jsonOverlapTemporalScale(leftText))
	rightValue, rightErr := types.ParseDatetime(rightText, jsonOverlapTemporalScale(rightText))
	if leftErr != nil || rightErr != nil {
		return strings.Compare(leftText, rightText)
	}
	return compareInt64(int64(leftValue), int64(rightValue))
}

func jsonOverlapTemporalScale(value string) int32 {
	dot := strings.LastIndexByte(value, '.')
	if dot < 0 {
		return 0
	}
	return int32(len(value) - dot - 1)
}

func compareInt(left, right int) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareInt64(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareUint64(left, right uint64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareFloat64Total(left, right float64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	if left == right {
		return 0
	}
	leftNaN := math.IsNaN(left)
	rightNaN := math.IsNaN(right)
	if leftNaN != rightNaN {
		if leftNaN {
			return 1
		}
		return -1
	}
	return compareUint64(math.Float64bits(left), math.Float64bits(right))
}
