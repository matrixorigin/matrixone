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

// Package keycodec defines the canonical byte contract shared by resident
// hashmaps and spill partitioning.
package keycodec

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ValidVectors verifies that each vector covers the requested logical rows.
// Scalar const vectors physically store one value and broadcast it across
// those rows; ordinary short or nil vectors must never leave a hash suffix at
// its seed.
func ValidVectors(vecs []*vector.Vector, rows int) bool {
	if rows < 0 || len(vecs) == 0 {
		return false
	}
	for _, vec := range vecs {
		if vec == nil {
			return false
		}
		if vec.IsConst() {
			if !vec.CoversLogicalRows(0, rows) {
				return false
			}
			continue
		}
		if vec.Length() != rows {
			return false
		}
	}
	return true
}

var groupingColumnHash = xxhash.Sum64([]byte{2})

// Float32Codec holds the SQL comparison normalization for one FLOAT32 type.
// Construct it once per vector so scale processing is not repeated per row.
type Float32Codec struct {
	normalizer types.Float32ScaleNormalizer
}

// ExactRuntimeFilterEncoding describes the closure a runtime-filter producer
// must apply before raw IN, zonemap, and persistent Bloom-filter consumers may
// use its payload.
type ExactRuntimeFilterEncoding uint8

const (
	ExactRuntimeFilterUnsupported ExactRuntimeFilterEncoding = iota
	ExactRuntimeFilterRaw
	ExactRuntimeFilterFloatZeroClosed
)

// SupportsExactRawRuntimeFilter reports whether a raw vector payload can be
// used as an exact join runtime filter for this type without transformation.
// Keep this as an explicit allowlist: a new type must prove that its raw
// representation preserves SQL join equality before it can opt in.
func SupportsExactRawRuntimeFilter(oid types.T) bool {
	switch oid {
	case types.T_bool,
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_uuid, types.T_year, types.T_enum:
		return true
	default:
		return false
	}
}

// LegacyExactRawProducerSafe reports whether pre-versioned producers and
// consumers can execute this raw contract using only RuntimeFilterSpec.Expr.
// Decimal equality depends on scale, and legacy consumers have no ENUM IN
// overload, so both require the versioned BuildExpr/ProbeType contract and
// make a legacy producer PASS.
func LegacyExactRawProducerSafe(oid types.T) bool {
	if !SupportsExactRawRuntimeFilter(oid) {
		return false
	}
	switch oid {
	case types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_enum:
		return false
	default:
		return true
	}
}

// ExactRuntimeFilterEncodingForPair returns the least transformation which
// makes payloadType a conservative exact filter for probeType. OIDs must match
// because the runtime message has no cross-type conversion contract. Width is
// representational metadata for the allowed string and fixed-width types.
//
// FLOAT64 and unscaled FLOAT32 need only signed-zero closure: non-NaN SQL
// equality otherwise implies identical bits. Scaled FLOAT32 remains
// unsupported because one rounded SQL value can cover many physical values;
// neither a raw IN vector nor a persisted Bloom filter can represent that
// interval without false negatives.
func ExactRuntimeFilterEncodingForPair(probeType, payloadType types.Type) ExactRuntimeFilterEncoding {
	if probeType.Oid != payloadType.Oid {
		return ExactRuntimeFilterUnsupported
	}
	switch probeType.Oid {
	case types.T_float32:
		if probeType.Scale <= 0 && payloadType.Scale <= 0 {
			return ExactRuntimeFilterFloatZeroClosed
		}
		return ExactRuntimeFilterUnsupported
	case types.T_float64:
		return ExactRuntimeFilterFloatZeroClosed
	}
	if !SupportsExactRawRuntimeFilter(probeType.Oid) {
		return ExactRuntimeFilterUnsupported
	}
	switch probeType.Oid {
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		if probeType.Scale != payloadType.Scale {
			return ExactRuntimeFilterUnsupported
		}
	default:
	}
	return ExactRuntimeFilterRaw
}

// SupportsExactRawRuntimeFilterPair reports whether no payload transformation
// is required for the pair.
func SupportsExactRawRuntimeFilterPair(probeType, payloadType types.Type) bool {
	return ExactRuntimeFilterEncodingForPair(probeType, payloadType) == ExactRuntimeFilterRaw
}

// NewFloat32Codec returns the canonical key codec for a FLOAT32 scale.
func NewFloat32Codec(scale int32) Float32Codec {
	return Float32Codec{
		normalizer: types.NewFloat32ScaleNormalizer(scale),
	}
}

// CanonicalBits makes SQL-equal FLOAT32 values use one hash key. Scaled
// values are normalized exactly as scalar comparisons normalize them, and
// signed zero uses the single all-zero representation.
func (c Float32Codec) CanonicalBits(value float32) uint32 {
	return CanonicalFloat32Bits(c.normalizer.Normalize(value))
}

// CanonicalFloat32Bits returns canonical bits for a FLOAT32 value after any
// required scale normalization. SQL equality identifies signed zero, so both
// zero representations use the single all-zero key.
func CanonicalFloat32Bits(value float32) uint32 {
	bits := math.Float32bits(value)
	if bits<<1 == 0 {
		return 0
	}
	return bits
}

// CanonicalBytes returns the native-endian bytes used by resident hashmaps and
// spill partitioning.
func (c Float32Codec) CanonicalBytes(value float32) [4]byte {
	bits := c.CanonicalBits(value)
	return *(*[4]byte)(unsafe.Pointer(&bits))
}

// CanonicalFloat64Bits makes SQL-equal signed zero values use one hash key.
func CanonicalFloat64Bits(value float64) uint64 {
	bits := math.Float64bits(value)
	if bits<<1 == 0 {
		return 0
	}
	return bits
}

// CanonicalFloat64Bytes returns the native-endian bytes used by the resident
// hashmaps and spill partitioning. Returning the bytes by value avoids exposing
// mutable package-global storage to callers.
func CanonicalFloat64Bytes(value float64) [8]byte {
	bits := CanonicalFloat64Bits(value)
	return *(*[8]byte)(unsafe.Pointer(&bits))
}

// CanonicalJSONSize returns the exact resident key size for one binary JSON
// value. Arrays and objects are walked recursively because scalar JSON
// equality also identifies numeric forms below the root.
func CanonicalJSONSize(value []byte) int {
	if len(value) == 0 {
		return 0
	}
	return canonicalByteJSONSize(types.DecodeJson(value))
}

func canonicalByteJSONSize(value bytejson.ByteJson) int {
	if size, ok := bytejson.CanonicalNumberSize(value); ok {
		return size
	}
	if size, ok := bytejson.CanonicalBinarySize(value); ok {
		return size
	}
	switch value.Type {
	case bytejson.TpCodeArray:
		size := 1 + 4
		for i := 0; i < value.GetElemCnt(); i++ {
			size += 4 + canonicalByteJSONSize(value.GetArrayElem(i))
		}
		return size
	case bytejson.TpCodeObject:
		size := 1 + 4
		for i := 0; i < value.GetElemCnt(); i++ {
			size += 4 + len(value.GetObjectKey(i))
			size += 4 + canonicalByteJSONSize(value.GetObjectVal(i))
		}
		return size
	default:
		return 1 + len(value.Data)
	}
}

// AppendCanonicalJSON appends the resident/spill key bytes for one JSON value.
// Binary JSON stores 1 as INT64 and 1.0 as FLOAT64 even though scalar JSON
// equality identifies them. Numeric nodes therefore use a private key-only
// domain, including inside arrays and objects; other scalar representations
// remain byte-for-byte compatible with their storage form.
func AppendCanonicalJSON(dst, value []byte) []byte {
	if len(value) == 0 {
		return dst
	}
	return appendCanonicalByteJSON(dst, types.DecodeJson(value))
}

func appendCanonicalByteJSON(dst []byte, value bytejson.ByteJson) []byte {
	if numeric, ok := bytejson.AppendCanonicalNumber(dst, value); ok {
		return numeric
	}
	if binaryValue, ok := bytejson.AppendCanonicalBinary(dst, value); ok {
		return binaryValue
	}

	switch value.Type {
	case bytejson.TpCodeArray:
		dst = append(dst, byte(value.Type))
		dst = appendCanonicalUint32(dst, uint32(value.GetElemCnt()))
		for i := 0; i < value.GetElemCnt(); i++ {
			lengthOffset := len(dst)
			dst = appendCanonicalUint32(dst, 0)
			valueOffset := len(dst)
			dst = appendCanonicalByteJSON(dst, value.GetArrayElem(i))
			binary.LittleEndian.PutUint32(dst[lengthOffset:], uint32(len(dst)-valueOffset))
		}
		return dst
	case bytejson.TpCodeObject:
		dst = append(dst, byte(value.Type))
		dst = appendCanonicalUint32(dst, uint32(value.GetElemCnt()))
		for i := 0; i < value.GetElemCnt(); i++ {
			key := value.GetObjectKey(i)
			dst = appendCanonicalUint32(dst, uint32(len(key)))
			dst = append(dst, key...)
			lengthOffset := len(dst)
			dst = appendCanonicalUint32(dst, 0)
			valueOffset := len(dst)
			dst = appendCanonicalByteJSON(dst, value.GetObjectVal(i))
			binary.LittleEndian.PutUint32(dst[lengthOffset:], uint32(len(dst)-valueOffset))
		}
		return dst
	default:
		dst = append(dst, byte(value.Type))
		return append(dst, value.Data...)
	}
}

func appendCanonicalUint32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

// AppendCanonicalVecF32 appends VECF32 key bytes with SQL-equal signed-zero
// elements mapped to the single all-zero representation. The input vector is
// never mutated and its byte length is preserved.
func AppendCanonicalVecF32(dst, value []byte) []byte {
	start := len(dst)
	dst = append(dst, value...)
	if len(value)%4 != 0 {
		return dst
	}
	canonical := dst[start:]
	for offset := 0; offset < len(canonical); offset += 4 {
		bits := binary.LittleEndian.Uint32(canonical[offset:])
		if bits<<1 == 0 {
			binary.LittleEndian.PutUint32(canonical[offset:], 0)
		}
	}
	return dst
}

// AppendCanonicalVecF64 is the FLOAT64-array counterpart of
// AppendCanonicalVecF32.
func AppendCanonicalVecF64(dst, value []byte) []byte {
	start := len(dst)
	dst = append(dst, value...)
	if len(value)%8 != 0 {
		return dst
	}
	canonical := dst[start:]
	for offset := 0; offset < len(canonical); offset += 8 {
		bits := binary.LittleEndian.Uint64(canonical[offset:])
		if bits<<1 == 0 {
			binary.LittleEndian.PutUint64(canonical[offset:], 0)
		}
	}
	return dst
}

// AppendCanonicalVecF16 appends BF16/FLOAT16 array bytes with both signed-zero
// representations mapped to zero. Both formats use the high bit as sign and
// all remaining bits as exponent/mantissa.
func AppendCanonicalVecF16(dst, value []byte) []byte {
	start := len(dst)
	dst = append(dst, value...)
	if len(value)%2 != 0 {
		return dst
	}
	canonical := dst[start:]
	for offset := 0; offset < len(canonical); offset += 2 {
		bits := binary.LittleEndian.Uint16(canonical[offset:])
		if bits<<1 == 0 {
			binary.LittleEndian.PutUint16(canonical[offset:], 0)
		}
	}
	return dst
}

// HashCombine merges one column hash into the hash state for a composite key.
func HashCombine(hash, columnHash uint64) uint64 {
	return hash ^ (columnHash + 0x9e3779b97f4a7c15 + (hash << 6) + (hash >> 2))
}

// ComputeXXHash computes the canonical partition hash for a set of typed key
// vectors. Type dispatch happens once per vector; row loops remain specialized
// so common raw-byte keys do not pay a per-row codec dispatch cost.
func ComputeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) {
	if len(hashValues) == 0 {
		return
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = seed
	}
	if len(keyVecs) == 0 {
		return
	}

	for _, vec := range keyVecs {
		if vec.GetGrouping().GetBitmap().CountRange(0, uint64(rowCount)) > 0 {
			computeGroupingXXHash(vec, hashValues)
			continue
		}
		switch vec.GetType().Oid {
		case types.T_float32:
			computeFloat32XXHash(vec, hashValues)
			continue
		case types.T_float64:
			computeFloat64XXHash(vec, hashValues)
			continue
		case types.T_json:
			computeCanonicalVarlenaXXHash(vec, hashValues, AppendCanonicalJSON)
			continue
		case types.T_array_float32:
			computeCanonicalVarlenaXXHash(vec, hashValues, AppendCanonicalVecF32)
			continue
		case types.T_array_float64:
			computeCanonicalVarlenaXXHash(vec, hashValues, AppendCanonicalVecF64)
			continue
		case types.T_array_bf16, types.T_array_float16:
			computeCanonicalVarlenaXXHash(vec, hashValues, AppendCanonicalVecF16)
			continue
		}
		if vec.IsConst() {
			columnHash := uint64(0)
			if !vec.IsConstNull() {
				columnHash = xxhash.Sum64(vec.GetRawBytesAt(0))
			}
			for i := 0; i < rowCount; i++ {
				hashValues[i] = HashCombine(hashValues[i], columnHash)
			}
			continue
		}

		n := rowCount
		if vec.Length() < n {
			n = vec.Length()
		}
		if vec.GetNulls().Any() {
			nulls := vec.GetNulls()
			for i := 0; i < n; i++ {
				if nulls.Contains(uint64(i)) {
					hashValues[i] = HashCombine(hashValues[i], 0)
				} else {
					hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
				}
			}
		} else {
			for i := 0; i < n; i++ {
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
			}
		}
	}
}

func computeCanonicalVarlenaXXHash(
	vec *vector.Vector,
	hashValues []uint64,
	appendCanonical func([]byte, []byte) []byte,
) {
	rowCount := len(hashValues)
	var scratch []byte
	if vec.IsConst() {
		columnHash := uint64(0)
		if !vec.IsConstNull() {
			scratch = appendCanonical(scratch, vec.GetRawBytesAt(0))
			columnHash = xxhash.Sum64(scratch)
		}
		for i := 0; i < rowCount; i++ {
			hashValues[i] = HashCombine(hashValues[i], columnHash)
		}
		return
	}

	n := min(rowCount, vec.Length())
	nulls := vec.GetNulls()
	for i := 0; i < n; i++ {
		if nulls.Contains(uint64(i)) {
			hashValues[i] = HashCombine(hashValues[i], 0)
			continue
		}
		scratch = appendCanonical(scratch[:0], vec.GetRawBytesAt(i))
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(scratch))
	}
}

func computeFloat32XXHash(vec *vector.Vector, hashValues []uint64) {
	rowCount := len(hashValues)
	codec := NewFloat32Codec(vec.GetType().Scale)
	if vec.IsConst() {
		columnHash := uint64(0)
		if !vec.IsConstNull() {
			values := vector.MustFixedColNoTypeCheck[float32](vec)
			value := codec.CanonicalBytes(values[0])
			columnHash = xxhash.Sum64(value[:])
		}
		for i := 0; i < rowCount; i++ {
			hashValues[i] = HashCombine(hashValues[i], columnHash)
		}
		return
	}

	n := rowCount
	if vec.Length() < n {
		n = vec.Length()
	}
	values := vector.MustFixedColNoTypeCheck[float32](vec)
	if vec.GetNulls().Any() {
		nulls := vec.GetNulls()
		for i := 0; i < n; i++ {
			if nulls.Contains(uint64(i)) {
				hashValues[i] = HashCombine(hashValues[i], 0)
			} else {
				value := codec.CanonicalBytes(values[i])
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		value := codec.CanonicalBytes(values[i])
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
	}
}

func computeGroupingXXHash(vec *vector.Vector, hashValues []uint64) {
	rowCount := len(hashValues)
	grouping := vec.GetGrouping()
	nulls := vec.GetNulls()
	var scratch []byte
	for i := 0; i < rowCount; i++ {
		if grouping.Contains(uint64(i)) {
			hashValues[i] = HashCombine(hashValues[i], groupingColumnHash)
			continue
		}
		if vec.IsConstNull() || nulls.Contains(uint64(i)) {
			hashValues[i] = HashCombine(hashValues[i], 0)
			continue
		}
		canonical, reusable := CanonicalBytesAt(vec, i, scratch[:0])
		scratch = reusable
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(canonical))
	}
}

// CanonicalBytesAt returns the byte representation used by SQL grouping
// equality. The caller owns null handling. canonical can alias vector storage;
// reusable preserves scratch capacity for the next call even in that case.
// Keeping this contract in keycodec prevents statistics, hash joins, and GROUP
// BY from silently defining different equality domains.
func CanonicalBytesAt(vec *vector.Vector, row int, scratch []byte) (canonical, reusable []byte) {
	if vec.IsConst() {
		row = 0
	}
	switch vec.GetType().Oid {
	case types.T_float32:
		values := vector.MustFixedColNoTypeCheck[float32](vec)
		value := NewFloat32Codec(vec.GetType().Scale).CanonicalBytes(values[row])
		scratch = append(scratch, value[:]...)
		return scratch, scratch
	case types.T_float64:
		values := vector.MustFixedColNoTypeCheck[float64](vec)
		value := CanonicalFloat64Bytes(values[row])
		scratch = append(scratch, value[:]...)
		return scratch, scratch
	case types.T_json:
		scratch = AppendCanonicalJSON(scratch, vec.GetRawBytesAt(row))
		return scratch, scratch
	case types.T_array_float32:
		scratch = AppendCanonicalVecF32(scratch, vec.GetRawBytesAt(row))
		return scratch, scratch
	case types.T_array_float64:
		scratch = AppendCanonicalVecF64(scratch, vec.GetRawBytesAt(row))
		return scratch, scratch
	case types.T_array_bf16, types.T_array_float16:
		scratch = AppendCanonicalVecF16(scratch, vec.GetRawBytesAt(row))
		return scratch, scratch
	default:
		return vec.GetRawBytesAt(row), scratch
	}
}

func computeFloat64XXHash(vec *vector.Vector, hashValues []uint64) {
	rowCount := len(hashValues)
	if vec.IsConst() {
		columnHash := uint64(0)
		if !vec.IsConstNull() {
			values := vector.MustFixedColNoTypeCheck[float64](vec)
			value := CanonicalFloat64Bytes(values[0])
			columnHash = xxhash.Sum64(value[:])
		}
		for i := 0; i < rowCount; i++ {
			hashValues[i] = HashCombine(hashValues[i], columnHash)
		}
		return
	}

	n := rowCount
	if vec.Length() < n {
		n = vec.Length()
	}
	values := vector.MustFixedColNoTypeCheck[float64](vec)
	if vec.GetNulls().Any() {
		nulls := vec.GetNulls()
		for i := 0; i < n; i++ {
			if nulls.Contains(uint64(i)) {
				hashValues[i] = HashCombine(hashValues[i], 0)
			} else {
				value := CanonicalFloat64Bytes(values[i])
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		value := CanonicalFloat64Bytes(values[i])
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
	}
}
