// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"encoding/base64"
	"io"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// WriteJSONText writes the visible JSON representation without allocating a
// payload-sized intermediate slice. It is byte-for-byte equivalent to
// ByteJson.MarshalJSON.
func WriteJSONText(w io.Writer, value ByteJson) error {
	switch value.Type {
	case TpCodeArray:
		if err := writeByte(w, '['); err != nil {
			return err
		}
		for idx := 0; idx < value.GetElemCnt(); idx++ {
			if idx > 0 {
				if err := writeString(w, ", "); err != nil {
					return err
				}
			}
			if err := WriteJSONText(w, value.GetArrayElem(idx)); err != nil {
				return err
			}
		}
		return writeByte(w, ']')
	case TpCodeObject:
		if err := writeByte(w, '{'); err != nil {
			return err
		}
		for idx := 0; idx < value.GetElemCnt(); idx++ {
			if idx > 0 {
				if err := writeString(w, ", "); err != nil {
					return err
				}
			}
			if err := WriteJSONString(w, value.GetObjectKey(idx)); err != nil {
				return err
			}
			if err := writeString(w, ": "); err != nil {
				return err
			}
			if err := WriteJSONText(w, value.GetObjectVal(idx)); err != nil {
				return err
			}
		}
		return writeByte(w, '}')
	case TpCodeInt64:
		var buf [32]byte
		return writeBytes(w, strconv.AppendInt(buf[:0], value.GetInt64(), 10))
	case TpCodeUint64:
		var buf [32]byte
		return writeBytes(w, strconv.AppendUint(buf[:0], value.GetUint64(), 10))
	case TpCodeLiteral:
		if len(value.Data) == 0 {
			return moerr.NewInvalidInputNoCtx("invalid JSON literal")
		}
		switch value.Data[0] {
		case LiteralNull:
			return writeString(w, "null")
		case LiteralTrue:
			return writeString(w, "true")
		case LiteralFalse:
			return writeString(w, "false")
		default:
			return moerr.NewInvalidInputNoCtxf("invalid JSON literal %d", value.Data[0])
		}
	case TpCodeFloat64:
		f := value.GetFloat64()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return moerr.NewInvalidInputNoCtxf("invalid JSON float64 %f", f)
		}
		format := byte('e')
		abs := math.Abs(f)
		if abs == 0 || 1e-6 <= abs && abs < 1e21 {
			format = 'f'
		}
		var buf [32]byte
		return writeBytes(w, strconv.AppendFloat(buf[:0], f, format, -1, 64))
	case TpCodeString:
		return WriteJSONString(w, value.GetString())
	case TpCodeDecimal:
		return writeBytes(w, value.GetString())
	case TpCodeDate, TpCodeTime, TpCodeDatetime:
		if err := writeByte(w, '"'); err != nil {
			return err
		}
		if err := writeBytes(w, value.GetString()); err != nil {
			return err
		}
		return writeByte(w, '"')
	case TpCodeBlob:
		if err := writeByte(w, '"'); err != nil {
			return err
		}
		if err := writeBinaryJSONText(w, value); err != nil {
			return err
		}
		return writeByte(w, '"')
	case TpCodeOpaque, TpCodeBit:
		if err := writeByte(w, '"'); err != nil {
			return err
		}
		if err := writeBinaryJSONText(w, value); err != nil {
			return err
		}
		return writeByte(w, '"')
	default:
		return moerr.NewInvalidInputNoCtxf("invalid JSON type %d", value.Type)
	}
}

// WriteJSONObjectKeyText applies JSON_OBJECT's key coercion to an existing
// binary-JSON value without allocating its visible representation.
func WriteJSONObjectKeyText(w io.Writer, value ByteJson) error {
	switch value.Type {
	case TpCodeString, TpCodeDate, TpCodeTime, TpCodeDatetime:
		return writeBytes(w, value.GetString())
	case TpCodeBlob, TpCodeOpaque, TpCodeBit:
		return writeBinaryJSONText(w, value)
	default:
		return WriteJSONText(w, value)
	}
}

// WriteJSONBase64Text writes the visible text of a binary JSON scalar without
// surrounding quotes.
func WriteJSONBase64Text(w io.Writer, value []byte) error {
	return writeRawBase64(w, value)
}

func writeBinaryJSONText(w io.Writer, value ByteJson) error {
	if value.Type == TpCodeOpaque || value.Type == TpCodeBit {
		return writeRawBase64(w, value.GetString())
	}
	data := value.GetString()
	if len(data) >= len(persistedBitPrefix) &&
		string(data[:len(persistedBitPrefix)]) == persistedBitPrefix {
		encoded := data[len(persistedBitPrefix):]
		if _, ok := base64DecodedLen(encoded); ok {
			return writeNormalizedBase64(w, encoded)
		}
	}
	return writeBytes(w, data)
}

// WriteJSONString writes one JSON string without allocating an escaped copy.
func WriteJSONString(w io.Writer, value []byte) error {
	if err := writeByte(w, '"'); err != nil {
		return err
	}
	start := 0
	for offset := 0; offset < len(value); {
		b := value[offset]
		if b < utf8.RuneSelf {
			if b >= ' ' && b != '"' && b != '\\' {
				offset++
				continue
			}
			if err := writeBytes(w, value[start:offset]); err != nil {
				return err
			}
			var escaped string
			switch b {
			case '"':
				escaped = `\"`
			case '\\':
				escaped = `\\`
			case '\b':
				escaped = `\b`
			case '\f':
				escaped = `\f`
			case '\n':
				escaped = `\n`
			case '\r':
				escaped = `\r`
			case '\t':
				escaped = `\t`
			default:
				const hex = "0123456789abcdef"
				var escapedControl = [6]byte{'\\', 'u', '0', '0', hex[b>>4], hex[b&0xf]}
				if err := writeBytes(w, escapedControl[:]); err != nil {
					return err
				}
				offset++
				start = offset
				continue
			}
			if err := writeString(w, escaped); err != nil {
				return err
			}
			offset++
			start = offset
			continue
		}
		_, size := utf8.DecodeRune(value[offset:])
		if size == 1 {
			return moerr.NewInvalidInputNoCtx("invalid UTF-8")
		}
		offset += size
	}
	if err := writeBytes(w, value[start:]); err != nil {
		return err
	}
	return writeByte(w, '"')
}

func writeNormalizedBase64(w io.Writer, encoded []byte) error {
	var decoded [binaryJSONCompareDecodedChunkSize]byte
	for offset := 0; offset < len(encoded); {
		n, next, ok := decodeBase64Chunk(encoded, offset, decoded[:])
		if !ok {
			return moerr.NewInvalidInputNoCtx("invalid base64 JSON value")
		}
		if err := writeRawBase64(w, decoded[:n]); err != nil {
			return err
		}
		offset = next
	}
	return nil
}

func writeRawBase64(w io.Writer, raw []byte) error {
	const decodedChunk = 3 * 256
	const encodedChunk = 4 * 256
	var encoded [encodedChunk]byte
	for len(raw) > 0 {
		length := min(len(raw), decodedChunk)
		if length < len(raw) {
			length -= length % 3
		}
		written := base64.StdEncoding.EncodedLen(length)
		base64.StdEncoding.Encode(encoded[:written], raw[:length])
		if err := writeBytes(w, encoded[:written]); err != nil {
			return err
		}
		raw = raw[length:]
	}
	return nil
}

func writeBytes(w io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := w.Write(value)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(value) {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}

func writeString(w io.Writer, value string) error {
	if stringWriter, ok := w.(io.StringWriter); ok {
		written, err := stringWriter.WriteString(value)
		if err != nil {
			return err
		}
		if written != len(value) {
			return io.ErrShortWrite
		}
		return nil
	}
	return writeBytes(w, []byte(value))
}

func writeByte(w io.Writer, value byte) error {
	if byteWriter, ok := w.(io.ByteWriter); ok {
		return byteWriter.WriteByte(value)
	}
	buffer := [1]byte{value}
	return writeBytes(w, buffer[:])
}
