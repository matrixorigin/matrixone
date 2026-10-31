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
	"context"
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// IsJSONNumericComparison identifies the mixed domain supported by the binary
// comparisons. BIT, YEAR and ENUM keep their independent conversion contracts.
// This domain cannot be used as a hash key or a persisted metadata probe:
// equality also depends on the numeric peer's floating-point domain.
func IsJSONNumericComparison(left, right types.T) bool {
	return left == types.T_json && isJSONComparisonNumericType(right) ||
		right == types.T_json && isJSONComparisonNumericType(left)
}

func isJSONComparisonNumericType(typ types.T) bool {
	switch typ {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return true
	}
	return false
}

func isJSONNumber(value bytejson.ByteJson) bool {
	switch value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return true
	}
	return false
}

// jsonNumericPeer is invocation-owned. Its bytes are either borrowed from an
// immutable input vector or from the caller's scalar scratch buffer. A constant
// decimal peer is parsed once, without copying the JSON document being compared.
type jsonNumericPeer struct {
	value       bytejson.ByteJson
	typ         types.T
	kind        vector.PrepareParamKind
	floatValue  float64
	parsed      bytejson.ParsedNumeric
	parsedReady bool
}

func makeJSONNumericPeer(ctx context.Context, value bytejson.ByteJson, typ types.T, kind vector.PrepareParamKind) (jsonNumericPeer, error) {
	peer := jsonNumericPeer{value: value, typ: typ, kind: kind}
	// Parameter metadata describes the peer, not the JSON data. A malformed
	// numeric adapter payload must still fail, even for a nonnumeric JSON row.
	if !isJSONNumber(value) {
		return peer, jsonCastErr(ctx, typ)
	}
	var err error
	peer.floatValue, err = checkJSONNumericDomain(ctx, value, typ, kind)
	if err != nil {
		return peer, err
	}
	if typ.IsDecimal() || typ == types.T_any && kind == vector.PrepareParamDecimal {
		var ok bool
		peer.parsed, ok = bytejson.ParseNumeric(value)
		if !ok {
			return peer, jsonCastErr(ctx, types.T_decimal256)
		}
		peer.parsedReady = true
	}
	return peer, nil
}

// checkJSONNumericDomain validates the numeric peer's declared adapter domain.
// For JSON rows it is used only for floating-point conversion; integer peers
// must not impose their CAST range on the JSON value being compared.
func checkJSONNumericDomain(ctx context.Context, value bytejson.ByteJson, typ types.T, kind vector.PrepareParamKind) (float64, error) {
	switch {
	case typ.IsSignedInt():
		v, _, ok := jsonToInt64Scalar(value)
		bits := typ.TypeLen() * 8
		limit := int64(uint64(math.MaxInt64) >> (64 - bits))
		if !ok || v < -limit-1 || v > limit {
			return 0, jsonCastErr(ctx, typ)
		}
	case typ.IsUnsignedInt():
		v, _, ok := jsonToUint64Scalar(value)
		limit := uint64(math.MaxUint64) >> (64 - typ.TypeLen()*8)
		if !ok || v > limit {
			return 0, jsonCastErr(ctx, typ)
		}
	case typ == types.T_float32:
		v, _, err := preparedJSONFloatScalar(ctx, value, typ)
		return v, err
	case typ == types.T_float64 || typ == types.T_any && kind == vector.PrepareParamFloat:
		v, _, err := preparedJSONFloatScalarWithLimit(ctx, value, types.T_float64, math.MaxFloat64, false)
		return v, err
	case typ == types.T_any && kind == vector.PrepareParamInteger:
		if _, ok := bytejson.NumericToInt64(value); !ok {
			if _, ok := bytejson.NumericToUint64(value); !ok {
				return 0, jsonCastErr(ctx, types.T_int64)
			}
		}
	}
	return 0, nil
}

func compareJSONNumeric(ctx context.Context, value bytejson.ByteJson, peer *jsonNumericPeer) (int, bool, error) {
	if isJSONLiteralNull(value) {
		return 0, true, nil
	}
	if !isJSONNumber(value) {
		// Both inputs came from admitted vectors. Different category ranks
		// decide this comparison without scanning any container descendants.
		return bytejson.CompareByteJsonTrusted(value, peer.value), false, nil
	}
	if peer.typ.IsFloat() || peer.typ == types.T_any && peer.kind == vector.PrepareParamFloat {
		floatValue, err := checkJSONNumericDomain(ctx, value, peer.typ, peer.kind)
		if err != nil {
			return 0, false, err
		}
		return compareFloat64Total(floatValue, peer.floatValue), false, nil
	}
	if peer.parsedReady {
		parsed, ok := bytejson.ParseNumeric(value)
		if !ok {
			return 0, false, jsonCastErr(ctx, types.T_decimal256)
		}
		cmp, ok := bytejson.CompareParsedNumeric(parsed, peer.parsed)
		if !ok {
			return 0, false, jsonCastErr(ctx, types.T_decimal256)
		}
		return cmp, false, nil
	}
	cmp, ok := bytejson.CompareNumeric(value, peer.value)
	if !ok {
		return 0, false, jsonCastErr(ctx, peer.typ)
	}
	return cmp, false, nil
}

// comparisonNumericAt encodes a fixed-width number into caller-owned scratch.
// Only decimal formatting needs a bounded per-value payload allocation.
func comparisonNumericAt(value *vector.Vector, row int, scratch *[8]byte) bytejson.ByteJson {
	code := bytejson.TpCodeInt64
	var bits uint64
	switch value.GetType().Oid {
	case types.T_int8:
		bits = uint64(vector.GetFixedAtNoTypeCheck[int8](value, row))
	case types.T_int16:
		bits = uint64(vector.GetFixedAtNoTypeCheck[int16](value, row))
	case types.T_int32:
		bits = uint64(vector.GetFixedAtNoTypeCheck[int32](value, row))
	case types.T_int64:
		bits = uint64(vector.GetFixedAtNoTypeCheck[int64](value, row))
	case types.T_uint8:
		code, bits = bytejson.TpCodeUint64, uint64(vector.GetFixedAtNoTypeCheck[uint8](value, row))
	case types.T_uint16:
		code, bits = bytejson.TpCodeUint64, uint64(vector.GetFixedAtNoTypeCheck[uint16](value, row))
	case types.T_uint32:
		code, bits = bytejson.TpCodeUint64, uint64(vector.GetFixedAtNoTypeCheck[uint32](value, row))
	case types.T_uint64:
		code, bits = bytejson.TpCodeUint64, vector.GetFixedAtNoTypeCheck[uint64](value, row)
	case types.T_float32:
		code, bits = bytejson.TpCodeFloat64, math.Float64bits(float64(vector.GetFixedAtNoTypeCheck[float32](value, row)))
	case types.T_float64:
		code, bits = bytejson.TpCodeFloat64, math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](value, row))
	case types.T_decimal64:
		return newTypedByteJson(bytejson.TpCodeDecimal, vector.GetFixedAtNoTypeCheck[types.Decimal64](value, row).Format(value.GetType().Scale))
	case types.T_decimal128:
		return newTypedByteJson(bytejson.TpCodeDecimal, vector.GetFixedAtNoTypeCheck[types.Decimal128](value, row).Format(value.GetType().Scale))
	case types.T_decimal256:
		return newTypedByteJson(bytejson.TpCodeDecimal, vector.GetFixedAtNoTypeCheck[types.Decimal256](value, row).Format(value.GetType().Scale))
	}
	binary.LittleEndian.PutUint64(scratch[:], bits)
	return bytejson.ByteJson{Type: code, Data: scratch[:]}
}

func compareJSONNumericVectors(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, nullSafe bool, cmp func(int) bool, selectList *FunctionSelectList) error {
	jsonPos, numericPos := 0, 1
	if parameters[0].GetType().Oid != types.T_json {
		jsonPos, numericPos = 1, 0
	}
	jsonVector, numericVector := parameters[jsonPos], parameters[numericPos]
	rs := vector.MustFunctionResult[bool](result)
	output := rs.GetResultVector()
	output.GetNulls().Reset()
	values := vector.MustFixedColNoTypeCheck[bool](output)
	var scratch [8]byte
	var peer jsonNumericPeer
	var peerReady bool
	var cachedJSON bytejson.ByteJson
	var cachedJSONNull, jsonReady bool
	for row := 0; row < length; row++ {
		values[row] = false
		if selectList != nil && (selectList.IgnoreAllRow() || selectList.Contains(uint64(row))) {
			output.GetNulls().Add(uint64(row))
			continue
		}
		value, jsonNull := cachedJSON, cachedJSONNull
		if !jsonReady {
			var err error
			value, jsonNull, err = comparisonJSONAt(proc, jsonVector, row)
			if err != nil {
				return err
			}
			if jsonVector.IsConst() {
				cachedJSON, cachedJSONNull, jsonReady = value, jsonNull, true
			}
		}
		jsonNull = jsonNull || isJSONLiteralNull(value)
		numericNull := numericVector.IsNull(uint64(row))
		if jsonNull || numericNull {
			if nullSafe {
				values[row] = jsonNull && numericNull
			} else {
				output.GetNulls().Add(uint64(row))
			}
			continue
		}
		if !peerReady {
			var err error
			peer, err = makeJSONNumericPeer(proc.Ctx, comparisonNumericAt(numericVector, row, &scratch), numericVector.GetType().Oid, vector.PrepareParamNone)
			if err != nil {
				return err
			}
			peerReady = numericVector.IsConst()
		}
		comparison, _, err := compareJSONNumeric(proc.Ctx, value, &peer)
		if err != nil {
			return err
		}
		if jsonPos == 1 {
			comparison = -comparison
		}
		values[row] = cmp(comparison)
	}
	return nil
}
