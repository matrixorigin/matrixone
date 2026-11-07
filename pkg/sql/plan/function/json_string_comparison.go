// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// IsJSONStringComparison identifies the mixed JSON/character-string domain of
// the binary comparisons. Binary strings keep their separate opaque contract.
// The domain is symmetric so both operand orders take the same path.
func IsJSONStringComparison(left, right types.T) bool {
	if left != types.T_json {
		left, right = right, left
	}
	return left == types.T_json && isJSONComparisonStringType(right)
}

func isJSONComparisonStringType(typ types.T) bool {
	switch typ {
	case types.T_char, types.T_varchar, types.T_text:
		return true
	}
	return false
}

// jsonBinaryStringComparisonNotSupported reports the mixed JSON operand and
// binary-string peer (BINARY, VARBINARY, BLOB). The character family joins the
// JSON domain as a string scalar, while the binary byte domains stay opaque:
// every ordering or equality operator on the pair rejects with the
// invalid-argument operator error, in both operand orders, mirroring the
// ordering gate jsonOrderingWithStringNotSupported that already covered the
// pair's ordering legs and reversed equality leg.
func jsonBinaryStringComparisonNotSupported(left, right types.T) bool {
	if left != types.T_json {
		left, right = right, left
	}
	return left == types.T_json && right.IsMySQLString() && !isJSONComparisonStringType(right)
}

// IsJSONBinaryStringComparisonNotSupported exposes the same mixed-pair
// classification for the dump-binding verifier. A persisted DEFAULT that
// pairs a JSON operand with a binary byte domain arrives only from a
// predecessor era: the current binder rejects the pair before binding, so
// the verifier must recognize it from the archived tree itself and select
// the era that accepts it.
func IsJSONBinaryStringComparisonNotSupported(left, right types.T) bool {
	return jsonBinaryStringComparisonNotSupported(left, right)
}

// appendStringScalarByteBuffer encodes payload with the varlena layout
// ByteJson expects for TpCodeString and returns the built scalar.
func appendStringScalarByteBuffer(scratch []byte, payload []byte) bytejson.ByteJson {
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], uint64(len(payload)))
	scratch = append(scratch, prefix[:n]...)
	scratch = append(scratch, payload...)
	return bytejson.ByteJson{Type: bytejson.TpCodeString, Data: scratch}
}

// compareJSONStringVectors implements the binary comparison operators for the
// JSON/string identity overloads. The SQL string operand becomes a JSON string
// scalar - the contract MySQL applies to a JSON value compared with SQL
// character data - and both operands compare through the type-ranked JSON
// order (null < number < string < object < array < boolean) instead of an
// implicit text cast that would conflate JSON numbers with unquoted JSON
// strings. SQL NULL still propagates per the SQL NULL contract, while JSON
// literal null stays a comparable value.
func compareJSONStringVectors(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	nullSafe bool,
	cmp func(int) bool,
	selectList *FunctionSelectList,
) error {
	jsonPos, strPos := 0, 1
	if parameters[0].GetType().Oid != types.T_json {
		jsonPos, strPos = 1, 0
	}
	jsonVector := parameters[jsonPos]
	strVector := parameters[strPos]
	rs := vector.MustFunctionResult[bool](result)
	output := rs.GetResultVector()
	output.GetNulls().Reset()
	values := vector.MustFixedColNoTypeCheck[bool](output)

	if selectList != nil && selectList.IgnoreAllRow() {
		nulls.AddRange(output.GetNulls(), 0, uint64(length))
		return nil
	}

	// A constant string operand owns a stable encoding; the loop's per-row
	// scratch rewrite cannot touch it.
	var constStr bytejson.ByteJson
	if strVector.IsConst() && !strVector.IsConstNull() {
		constStr = appendStringScalarByteBuffer(nil, strVector.GetBytesAt(0))
	}
	var constJSON bytejson.ByteJson
	var constJSONNull bool
	constJSONReady := false
	if jsonVector.IsConst() && !jsonVector.IsConstNull() {
		bj, isNull, err := comparisonJSONAt(proc, jsonVector, 0)
		if err != nil {
			return err
		}
		constJSON, constJSONNull, constJSONReady = bj, isNull, true
	}
	var scratch []byte

	for row := 0; row < length; row++ {
		values[row] = false
		if selectList != nil && (selectList.IgnoreAllRow() || selectList.Contains(uint64(row))) {
			output.GetNulls().Add(uint64(row))
			continue
		}

		jsonValue, jsonNull := bytejson.ByteJson{}, true
		if jsonVector.IsConst() {
			if constJSONReady {
				jsonValue, jsonNull = constJSON, constJSONNull
			}
		} else {
			var err error
			jsonValue, jsonNull, err = comparisonJSONAt(proc, jsonVector, row)
			if err != nil {
				return err
			}
		}

		strNull := strVector.IsNull(uint64(row))
		var strPeer bytejson.ByteJson
		if !strNull {
			if strVector.IsConst() {
				strPeer = constStr
			} else {
				strPeer = appendStringScalarByteBuffer(scratch[:0], strVector.GetBytesAt(row))
				scratch = strPeer.Data
			}
		}

		if jsonNull || strNull {
			if nullSafe {
				values[row] = jsonNull && strNull
			} else {
				output.GetNulls().Add(uint64(row))
			}
			continue
		}

		comparison := bytejson.CompareByteJsonTrusted(jsonValue, strPeer)
		if jsonPos == 1 {
			comparison = -comparison
		}
		values[row] = cmp(comparison)
	}
	return nil
}
