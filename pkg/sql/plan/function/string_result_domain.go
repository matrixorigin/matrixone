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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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

// CharacterSliceLiteralWidth refines only the width of a selected character
// result. The caller must establish an exclusively text runtime domain; this
// pure helper does not infer provenance or change the selected type family.
func CharacterSliceLiteralWidth(source *planpb.Expr, length *planpb.Literal, selected types.Type) (int32, bool) {
	if source == nil || length == nil || length.Isnull ||
		(selected.Oid != types.T_char && selected.Oid != types.T_varchar) || selected.Width < 0 {
		return 0, false
	}
	if oid := types.T(source.Typ.Id); oid != types.T_char && oid != types.T_varchar {
		return 0, false
	}
	var requested uint64
	var signed int64
	switch value := length.Value.(type) {
	case *planpb.Literal_I8Val:
		signed = int64(value.I8Val)
	case *planpb.Literal_I16Val:
		signed = int64(value.I16Val)
	case *planpb.Literal_I32Val:
		signed = int64(value.I32Val)
	case *planpb.Literal_I64Val:
		signed = value.I64Val
	case *planpb.Literal_U8Val:
		requested = uint64(value.U8Val)
	case *planpb.Literal_U16Val:
		requested = uint64(value.U16Val)
	case *planpb.Literal_U32Val:
		requested = uint64(value.U32Val)
	case *planpb.Literal_U64Val:
		requested = value.U64Val
	default:
		return 0, false
	}
	if signed > 0 {
		requested = uint64(signed)
	}
	sourceBound, known := TextSourceCharacterBound(source)
	if !known {
		return 0, false
	}
	bound := min(sourceBound, requested)
	if bound <= uint64(types.MaxVarcharLen) && bound < uint64(selected.Width) {
		return int32(bound), true
	}
	return 0, false
}

// TextSourceCharacterBound returns a conservative character-count bound for a
// value before a string-domain cast. Character declarations count characters;
// fixed scalar values use the same formatted-string bound as the cast planner.
// Binary declarations are intentionally not treated as text.
func TextSourceCharacterBound(expr *planpb.Expr) (uint64, bool) {
	if expr == nil {
		return 0, false
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.GetValue().(*planpb.Literal_Sval); ok {
			return uint64(utf8.RuneCountInString(value.Sval)), true
		}
	}

	sourceType := types.T(expr.Typ.Id)
	switch sourceType {
	case types.T_char, types.T_varchar, types.T_text:
		if expr.Typ.Width > 0 {
			return uint64(expr.Typ.Width), true
		}
		return 0, false
	case types.T_binary, types.T_varbinary, types.T_blob:
		return 0, false
	default:
		bound := formattedStringByteBound(types.Type{
			Oid:   sourceType,
			Width: expr.Typ.Width,
			Scale: expr.Typ.Scale,
		})
		if !bound.unknown {
			return bound.bytes, true
		}
	}
	return 0, false
}

// base64ResultBound mirrors encodeBase64WithLineBreaks.  The executor emits a
// line feed after every complete 76-character output line, so the ordinary
// four-thirds estimate is not sufficient for the persisted VARCHAR width.
func base64ResultBound(input stringResultBound) stringResultBound {
	if input.unknown || input.bytes > math.MaxUint64-2 {
		return unknownStringResultBound()
	}
	encoded := multiplyStringResultBound(
		stringResultBound{bytes: (input.bytes + 2) / 3}, 4)
	if encoded.unknown || encoded.bytes == 0 {
		return encoded
	}
	return addStringResultBounds(encoded, stringResultBound{bytes: (encoded.bytes - 1) / 76})
}

func base64ReturnType(parameters []types.Type) types.Type {
	if len(parameters) == 0 {
		return types.T_text.ToType()
	}
	// Base64 is ASCII.  declaredStringByteBound measures binary inputs in bytes
	// and character declarations conservatively in their maximum UTF-8 bytes.
	return textStringResultType(
		base64ResultBound(declaredStringByteBound(parameters[0])), types.CharsetUTF8)
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

// soundexReturnType keeps the result column large enough for Soundex's
// variable-length output. Text Soundex preserves its first qualifying Unicode
// character, so declared width is measured in characters while the encoded
// result may need up to seven bytes for a four-character code. The vector
// result grows by encoded bytes at execution time. Unbounded inputs use
// LONGTEXT because a plain TEXT result could be too narrow when the input
// itself comes from a widening expression.
func soundexReturnType(parameters []types.Type) types.Type {
	if len(parameters) != 1 {
		return types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8)
	}

	source := parameters[0]
	if types.StaticStringDomain(source) == types.StringDomainBinary {
		bound := declaredStringByteBound(source)
		if !bound.unknown && bound.bytes < 4 {
			bound.bytes = 4
		}
		return binaryStringResultType(bound)
	}
	if (source.Oid != types.T_char && source.Oid != types.T_varchar && source.Oid != types.T_text) || source.Width <= 0 {
		return types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8)
	}

	bound := uint64(source.Width)
	if bound < 4 {
		bound = 4
	}
	if bound <= uint64(types.MaxVarcharLen) {
		return types.NewWithCharset(types.T_varchar, int32(bound), 0, types.CharsetUTF8)
	}

	textWidth := int32(types.MaxLongTextLen)
	if bound <= uint64(types.MaxMediumTextLen) {
		textWidth = int32(types.MaxMediumTextLen)
	}
	return types.NewWithCharset(types.T_text, textWidth, 0, types.CharsetUTF8)
}

// octalResultType matches MySQL's OCT metadata. OCT returns a character
// representation rather than a numeric value; 65 is the stable declared
// VARCHAR width used by MySQL for this function, including the
// sign/two's-complement representation of a BIGINT.
func octalResultType(_ []types.Type) types.Type {
	return types.New(types.T_varchar, 65, 0)
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
