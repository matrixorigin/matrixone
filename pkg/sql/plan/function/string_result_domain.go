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
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// stringResultBound is a planner-only payload bound. unknown is deliberately
// different from zero: zero is the exact bound of an empty result.
type stringResultBound struct {
	bytes   uint64
	unknown bool
}

func unknownStringResultBound() stringResultBound { return stringResultBound{unknown: true} }

func addStringResultBounds(left, right stringResultBound) stringResultBound {
	if left.unknown || right.unknown || math.MaxUint64-left.bytes < right.bytes {
		return unknownStringResultBound()
	}
	return stringResultBound{bytes: left.bytes + right.bytes}
}

func multiplyStringResultBound(bound stringResultBound, count uint64) stringResultBound {
	if bound.unknown || (count != 0 && bound.bytes > math.MaxUint64/count) {
		return unknownStringResultBound()
	}
	return stringResultBound{bytes: bound.bytes * count}
}

// declaredStringByteBound converts a declared string type into a stored-byte
// bound. Width zero on TEXT/BLOB is unbounded; CHAR/VARCHAR widths count UTF-8
// characters, while BINARY/VARBINARY widths already count bytes.
func declaredStringByteBound(typ types.Type) stringResultBound {
	if typ.Width < 0 {
		return unknownStringResultBound()
	}
	switch typ.Oid {
	case types.T_char, types.T_varchar:
		return multiplyStringResultBound(stringResultBound{bytes: uint64(typ.Width)}, utf8.UTFMax)
	case types.T_binary, types.T_varbinary:
		return stringResultBound{bytes: uint64(typ.Width)}
	case types.T_text, types.T_blob:
		if typ.Width == 0 {
			return unknownStringResultBound()
		}
		return stringResultBound{bytes: uint64(typ.Width)}
	default:
		return unknownStringResultBound()
	}
}

// formattedStringByteBound is the maximum byte count produced when a fixed
// scalar is converted to its SQL string representation. Variable and unknown
// representations fail closed to an unbounded result.
func declaredTextCharacterBound(typ types.Type) stringResultBound {
	if typ.Width < 0 {
		return unknownStringResultBound()
	}
	switch typ.Oid {
	case types.T_char, types.T_varchar:
		return stringResultBound{bytes: uint64(typ.Width)}
	case types.T_text:
		if typ.Width == 0 {
			return unknownStringResultBound()
		}
		return stringResultBound{bytes: uint64(typ.Width)}
	default:
		// Every formatted byte can be at most one result character, so the byte
		// bound is also a conservative character bound for non-string values.
		return formattedStringByteBound(typ)
	}
}

func formattedStringByteBound(typ types.Type) stringResultBound {
	if typ.Oid.IsMySQLString() {
		return declaredStringByteBound(typ)
	}
	var width uint64
	switch typ.Oid {
	case types.T_bool:
		width = 1
	case types.T_int8:
		width = 4
	case types.T_int16:
		width = 6
	case types.T_int32:
		width = 11
	case types.T_int64:
		width = 20
	case types.T_uint8:
		width = 3
	case types.T_uint16:
		width = 5
	case types.T_uint32:
		width = 10
	case types.T_uint64, types.T_bit:
		width = 20
	case types.T_float32:
		// floatToBytes uses fixed notation for values in [1e-13, 1e15).
		// Keep room for the sign, the decimal point, leading fractional
		// zeroes, and the maximum exact FLOAT32 digits.
		width = 24
	case types.T_float64:
		// See FLOAT32 above. DOUBLE needs up to 17 exact significant digits.
		width = 32
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		if typ.Width <= 0 {
			return unknownStringResultBound()
		}
		width = uint64(typ.Width) + 2 // optional sign and decimal point
		if typ.Scale == typ.Width {
			width++ // leading zero before the decimal point
		}
	case types.T_date:
		width = 10
	case types.T_time:
		width = 17
	case types.T_datetime, types.T_timestamp:
		width = 26
	case types.T_year:
		width = 4
	case types.T_uuid:
		width = 36
	case types.T_enum:
		width = 5
	default:
		return unknownStringResultBound()
	}
	return stringResultBound{bytes: width}
}

func formattedScalarStringType(typ types.Type) types.Type {
	result := types.T_varchar.ToType()
	bound := formattedStringByteBound(typ)
	if !bound.unknown && bound.bytes <= uint64(types.MaxVarcharLen) {
		result.Width = int32(bound.bytes)
	}
	return result
}

func binaryStringResultType(bound stringResultBound) types.Type {
	if bound.unknown || bound.bytes > uint64(types.MaxVarBinaryLen) {
		return types.T_blob.ToType()
	}
	return types.NewWithCharset(types.T_varbinary, int32(bound.bytes), 0, types.CharsetBinary)
}

func fixedBinaryResultType(width int32) types.Type {
	return binaryStringResultType(stringResultBound{bytes: uint64(width)})
}

func fixedTextResultType(width uint64) types.Type {
	return textStringResultType(stringResultBound{bytes: width}, types.CharsetUTF8)
}

// compressResultBound mirrors zlib's compressBound contract, the five
// bytes added by MySQL's COMPRESS framing (four-byte length plus the optional
// trailing dot), and the two-byte final empty block emitted by Go's zlib
// writer. The latter is important: using MySQL's C-zlib bound verbatim can
// understate the actual MatrixOne result for incompressible input. Keep the
// arithmetic checked: a type callback must never wrap a large input into a
// deceptively small VARCHAR/VARBINARY.
func compressResultBound(input stringResultBound) stringResultBound {
	if input.unknown {
		return unknownStringResultBound()
	}

	bound := input
	for _, divisor := range []uint64{1 << 12, 1 << 14, 1 << 25} {
		bound = addStringResultBounds(bound, stringResultBound{bytes: input.bytes / divisor})
	}
	return addStringResultBounds(bound, stringResultBound{bytes: 20})
}

func roundedUpHalfStringResultBound(input stringResultBound) stringResultBound {
	if input.unknown {
		return unknownStringResultBound()
	}
	return stringResultBound{bytes: input.bytes/2 + input.bytes%2}
}

func aesPaddedResultBound(input stringResultBound) stringResultBound {
	if input.unknown {
		return unknownStringResultBound()
	}
	blocks := stringResultBound{bytes: input.bytes/16 + 1}
	return multiplyStringResultBound(blocks, 16)
}

func fixedVarcharReturnType(width uint64) types.Type {
	return fixedTextResultType(width)
}

func binReturnType(_ []types.Type) types.Type {
	return fixedVarcharReturnType(65)
}

func convReturnType(_ []types.Type) types.Type {
	return fixedVarcharReturnType(65)
}

func inetNtoaReturnType(_ []types.Type) types.Type {
	return fixedVarcharReturnType(31)
}

func inet6NtoaReturnType(_ []types.Type) types.Type {
	return fixedVarcharReturnType(39)
}

func numericHexReturnType(_ []types.Type) types.Type {
	return fixedVarcharReturnType(16)
}

func stringHexReturnType(parameters []types.Type) types.Type {
	if len(parameters) == 0 {
		return types.T_text.ToType()
	}
	return textStringResultType(
		multiplyStringResultBound(declaredStringByteBound(parameters[0]), 2),
		types.CharsetUTF8,
	)
}

func arrayHexReturnType(_ []types.Type) types.Type {
	// HexArray encodes the vector's serialized bytes. Its maximum byte count
	// depends on the element type and dimension, and can exceed VARCHAR(65535);
	// retain a lossless text domain until a dedicated array-byte bound exists.
	return types.T_text.ToType()
}

func unhexReturnType(parameters []types.Type) types.Type {
	if len(parameters) == 0 {
		return types.T_blob.ToType()
	}
	return binaryStringResultType(roundedUpHalfStringResultBound(
		declaredStringByteBound(parameters[0])))
}

func compressReturnType(parameters []types.Type) types.Type {
	if len(parameters) == 0 {
		return types.T_blob.ToType()
	}
	return binaryStringResultType(compressResultBound(
		declaredStringByteBound(parameters[0])))
}

func aesEncryptReturnType(parameters []types.Type) types.Type {
	if len(parameters) == 0 {
		return types.T_blob.ToType()
	}
	return binaryStringResultType(aesPaddedResultBound(
		declaredStringByteBound(parameters[0])))
}

func textStringResultType(bound stringResultBound, charset uint8) types.Type {
	if bound.unknown || bound.bytes > uint64(types.MaxVarcharLen) {
		result := types.T_text.ToType()
		result.Charset = charset
		return result
	}
	result := types.NewWithCharset(types.T_varchar, int32(bound.bytes), 0, charset)
	return result
}

func concatTextResultBound(parameters []types.Type, start int) stringResultBound {
	if start < 0 || start > len(parameters) {
		return unknownStringResultBound()
	}
	bound := stringResultBound{}
	for _, parameter := range parameters[start:] {
		bound = addStringResultBounds(bound, declaredTextCharacterBound(parameter))
	}
	return bound
}

func concatResultBound(parameters []types.Type, start int) stringResultBound {
	if start < 0 || start > len(parameters) {
		return unknownStringResultBound()
	}
	bound := stringResultBound{}
	for _, parameter := range parameters[start:] {
		bound = addStringResultBounds(bound, formattedStringByteBound(parameter))
	}
	return bound
}

func hasBinaryStringDomain(parameters []types.Type) bool {
	for _, parameter := range parameters {
		if types.StaticStringDomain(parameter) == types.StringDomainBinary {
			return true
		}
	}
	return false
}
