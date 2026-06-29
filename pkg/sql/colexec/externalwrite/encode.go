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

package externalwrite

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var (
	csvNull   = []byte("\\N")
	csvTrue   = []byte("true")
	csvFalse  = []byte("false")
	jsonNull  = []byte("null")
	jsonTrue  = []byte("true")
	jsonFalse = []byte("false")
)

// encodeCSV renders every row of bat as a CSV record. The per-type formatting
// mirrors the SELECT INTO OUTFILE encoder (pkg/frontend/export.go constructByte)
// so the output round-trips through the external-table CSV reader.
// The returned slice aliases w.buf and is only valid until the next encode.
func (w *externalWriter) encodeCSV(bat *batch.Batch) ([]byte, error) {
	buf := &w.buf
	buf.Reset()
	enclosed := w.cfg.EnclosedBy
	escape := w.cfg.escapeChar()
	// Only the table's columns are written; the pipeline may carry trailing
	// hidden vectors (mirrors insert_table, which copies only InsertCtx.Attrs).
	ncol := w.colCount(bat)

	for i := 0; i < bat.RowCount(); i++ {
		buf.Write(w.cfg.LineStartingBy)
		for j := 0; j < ncol; j++ {
			vec := bat.Vecs[j]
			last := j == ncol-1
			if vec.IsNull(uint64(i)) {
				// The NULL sentinel is written verbatim (never escaped): the
				// reader matches it as the raw token \N.
				w.writeCSVField(buf, csvNull, false, last)
				continue
			}
			val, quote, err := w.csvValue(vec, i)
			if err != nil {
				return nil, err
			}
			// Values no encoding can round-trip are rejected rather than
			// silently corrupted (see the reader's unescapeString / record-CR
			// handling):
			// - content exactly \N null-matches even when enclosed, and only
			//   the default backslash escape flavor exempts the written \\N;
			// - with escaping disabled, a CR as the record's last byte is eaten
			//   by the reader's end-of-record CR strip.
			if escape != '\\' && bytes.Equal(val, csvNull) {
				return nil, moerr.NewNotSupported(context.Background(),
					"external write (csv): a value of exactly \\N cannot round-trip unless FIELDS ESCAPED BY is the default '\\'")
			}
			if escape == 0 && last && bytes.HasSuffix(val, []byte{'\r'}) {
				return nil, moerr.NewNotSupported(context.Background(),
					"external write (csv): with ESCAPED BY '' a value ending in CR in the last column cannot round-trip")
			}
			// Values written unenclosed must not collide with the reader's
			// tokenization: enclose them when they contain a structural byte
			// (OPTIONALLY ENCLOSED semantics). E.g. FIELDS TERMINATED BY '-'
			// would otherwise split an unenclosed DATE into three fields.
			if !quote && w.needsEnclosure(val) {
				quote = true
			}
			// COMMENT guard (first column only): the reader skips a line whose raw
			// prefix matches the marker. If leaving this field unenclosed would
			// make the line start with the marker, enclose it — the line then
			// begins with the enclosure byte and is read as data. Only the first
			// field can begin the line, and we enclose only on a real collision.
			if j == 0 && !quote && len(w.cfg.Comment) > 0 &&
				w.firstFieldStartsComment(addEscape(val, 0, escape), ncol == 1) {
				quote = true
			}
			// The reader unescapes UNQUOTED fields too, so every value must be
			// escaped — with a non-default escape char like '-', even a date
			// would otherwise lose bytes to the reader's E-sequence collapsing.
			// Enclosure doubling only matters inside an enclosed field.
			encloseFor := byte(0)
			if quote {
				encloseFor = enclosed
			}
			val = addEscape(val, encloseFor, escape)
			w.writeCSVField(buf, val, quote, last)
		}
	}
	return buf.Bytes(), nil
}

// needsEnclosure reports whether an otherwise-unenclosed value must be
// enclosed to survive the reader's tokenization. The reader's unquoted-field
// scanner stops on the enclosure byte, the field-terminator sequence, and any
// byte of the line terminator (a \r\n-style terminator accepts \r or \n alone
// as a record end), so values containing them are written enclosed instead.
// For a multi-char field terminator the reader also matches across the
// value/terminator boundary (value '10' + terminator '00' scans as '1'+'00'),
// so a value whose suffix is a proper prefix of the terminator is enclosed too.
func (w *externalWriter) needsEnclosure(val []byte) bool {
	if w.cfg.EnclosedBy != 0 && bytes.IndexByte(val, w.cfg.EnclosedBy) >= 0 {
		return true
	}
	term := w.cfg.FieldTerminator
	if len(term) > 0 && bytes.Contains(val, term) {
		return true
	}
	for k := min(len(val), len(term)-1); k >= 1; k-- {
		if bytes.HasSuffix(val, term[:k]) {
			return true
		}
	}
	for _, b := range w.cfg.LineTerminator {
		if bytes.IndexByte(val, b) >= 0 {
			return true
		}
	}
	return false
}

// firstFieldStartsComment reports whether writing field (the escaped, unenclosed
// first-column bytes) would make the line's raw prefix match the COMMENT marker,
// so the reader would skip the row. The candidate prefix is the field bytes then
// the byte that follows it (the field terminator, or the line terminator when it
// is the only column). COMMENT is mutually exclusive with LINES STARTING BY for
// writable tables (rejected at DDL by validateWritableComment), so there is no
// starting-by prefix to account for here; the other unprotectable cases (the
// enclosure byte, the NULL sentinel) are likewise rejected at DDL, so this guard
// only handles a non-NULL unenclosed first value that itself begins the marker.
func (w *externalWriter) firstFieldStartsComment(field []byte, onlyColumn bool) bool {
	c := w.cfg.Comment
	if len(c) == 0 {
		return false
	}
	prefix := make([]byte, 0, len(field)+len(w.cfg.FieldTerminator))
	prefix = append(prefix, field...)
	if onlyColumn {
		prefix = append(prefix, w.cfg.LineTerminator...)
	} else {
		prefix = append(prefix, w.cfg.FieldTerminator...)
	}
	return bytes.HasPrefix(prefix, c)
}

func (w *externalWriter) writeCSVField(buf *bytes.Buffer, value []byte, quote bool, last bool) {
	enclosed := w.cfg.EnclosedBy
	if quote && enclosed != 0 {
		buf.WriteByte(enclosed)
	}
	buf.Write(value)
	if quote && enclosed != 0 {
		buf.WriteByte(enclosed)
	}
	if last {
		buf.Write(w.cfg.LineTerminator)
	} else {
		buf.Write(w.cfg.FieldTerminator)
	}
}

func (w *externalWriter) writeCSVHeader() error {
	buf := &bytes.Buffer{}
	ncol := len(w.cfg.Attrs)
	for j, name := range w.cfg.Attrs {
		w.writeCSVField(buf, []byte(name), w.cfg.EnclosedBy != 0, j == ncol-1)
	}
	_, err := w.fw.Write(buf.Bytes())
	return err
}

// csvValue formats a single non-null cell to bytes. quote indicates whether the
// value is string-like and should be wrapped in the enclosure char (matching the
// export encoder, which always encloses string/binary/json/array values).
func (w *externalWriter) csvValue(vec *vector.Vector, i int) (val []byte, quote bool, err error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		if vector.GetFixedAtNoTypeCheck[bool](vec, i) {
			return csvTrue, false, nil
		}
		return csvFalse, false, nil
	case types.T_bit:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
		bitLength := vec.GetType().Width
		byteLength := (bitLength + 7) / 8
		b := types.EncodeUint64(&v)[:byteLength]
		b = slices.Clone(b)
		slices.Reverse(b)
		// quote=true: the raw bytes can contain the field/line terminator or the
		// enclosure char, so they must be enclosed and escaped like binary values.
		return b, true, nil
	case types.T_int8:
		return []byte(strconv.FormatInt(int64(vector.GetFixedAtNoTypeCheck[int8](vec, i)), 10)), false, nil
	case types.T_int16:
		return []byte(strconv.FormatInt(int64(vector.GetFixedAtNoTypeCheck[int16](vec, i)), 10)), false, nil
	case types.T_int32:
		return []byte(strconv.FormatInt(int64(vector.GetFixedAtNoTypeCheck[int32](vec, i)), 10)), false, nil
	case types.T_int64:
		return []byte(strconv.FormatInt(vector.GetFixedAtNoTypeCheck[int64](vec, i), 10)), false, nil
	case types.T_uint8:
		return []byte(strconv.FormatUint(uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, i)), 10)), false, nil
	case types.T_uint16:
		return []byte(strconv.FormatUint(uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, i)), 10)), false, nil
	case types.T_uint32:
		return []byte(strconv.FormatUint(uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), 10)), false, nil
	case types.T_uint64:
		return []byte(strconv.FormatUint(vector.GetFixedAtNoTypeCheck[uint64](vec, i), 10)), false, nil
	case types.T_float32:
		v := vector.GetFixedAtNoTypeCheck[float32](vec, i)
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			return []byte(strconv.FormatFloat(float64(v), 'f', -1, 32)), false, nil
		}
		return []byte(strconv.FormatFloat(float64(v), 'f', int(vec.GetType().Scale), 32)), false, nil
	case types.T_float64:
		v := vector.GetFixedAtNoTypeCheck[float64](vec, i)
		if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
			return []byte(strconv.FormatFloat(v, 'f', -1, 64)), false, nil
		}
		return []byte(strconv.FormatFloat(v, 'f', int(vec.GetType().Scale), 64)), false, nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink:
		return vec.GetBytesAt(i), true, nil
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		return []byte(val.String()), true, nil
	case types.T_array_float32:
		return []byte(types.BytesToArrayToString[float32](vec.GetBytesAt(i))), true, nil
	case types.T_array_float64:
		return []byte(types.BytesToArrayToString[float64](vec.GetBytesAt(i))), true, nil
	case types.T_date:
		return []byte(vector.GetFixedAtNoTypeCheck[types.Date](vec, i).String()), false, nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)), false, nil
	case types.T_time:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)), false, nil
	case types.T_timestamp:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(w.cfg.TimeZone, scale)), false, nil
	case types.T_year:
		return []byte(vector.GetFixedAtNoTypeCheck[types.MoYear](vec, i).String()), false, nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)), false, nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)), false, nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		return []byte(vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale)), false, nil
	case types.T_uuid:
		return []byte(vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()), false, nil
	case types.T_enum:
		// quote=true: enum labels are user-defined strings; an unenclosed label
		// 'NULL' would read back as SQL NULL (the reader maps a bare unquoted
		// NULL token to null when an enclosure is configured).
		return []byte(vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()), true, nil
	default:
		return nil, false, moerr.NewInternalErrorf(context.Background(),
			"external write (csv): unsupported column type %s", vec.GetType().String())
	}
}

// encodeJSONLine renders every row of bat as a JSONLine record: one JSON
// object per line, keys in declared-column order, lines separated by the
// configured line terminator. Values are appended directly to the buffer (no
// per-row map, boxing, or reflection — this runs per cell on the bulk
// INSERT/LOAD path). The returned slice aliases w.buf and is only valid until
// the next encode.
func (w *externalWriter) encodeJSONLine(bat *batch.Batch) ([]byte, error) {
	// JSON objects need key names; without Attrs there is nothing to emit (and
	// colCount's all-vectors fallback would index past jsonKeys).
	if len(w.cfg.Attrs) == 0 {
		return nil, moerr.NewInternalError(context.Background(),
			"external write (jsonline): writer configured without column names")
	}
	buf := &w.buf
	buf.Reset()
	ncol := w.colCount(bat)
	if w.jsonKeys == nil {
		w.jsonKeys = make([][]byte, len(w.cfg.Attrs))
		var kb bytes.Buffer
		for j, name := range w.cfg.Attrs {
			kb.Reset()
			appendJSONString(&kb, []byte(name))
			kb.WriteByte(':')
			w.jsonKeys[j] = bytes.Clone(kb.Bytes())
		}
	}
	for i := 0; i < bat.RowCount(); i++ {
		buf.Write(w.cfg.LineStartingBy)
		buf.WriteByte('{')
		for j := 0; j < ncol; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.Write(w.jsonKeys[j])
			vec := bat.Vecs[j]
			if vec.IsNull(uint64(i)) {
				buf.Write(jsonNull)
				continue
			}
			if err := w.appendJSONValue(buf, vec, i); err != nil {
				return nil, err
			}
		}
		buf.WriteByte('}')
		buf.Write(w.cfg.LineTerminator)
	}
	return buf.Bytes(), nil
}

// appendJSONValue appends row i of vec to buf as a JSON value.
func (w *externalWriter) appendJSONValue(buf *bytes.Buffer, vec *vector.Vector, i int) error {
	switch vec.GetType().Oid {
	case types.T_json:
		// Compact to match the reader's (and the previous json.Marshal
		// round-trip's) canonical form.
		val := types.DecodeJson(vec.GetBytesAt(i))
		if err := json.Compact(buf, []byte(val.String())); err != nil {
			return moerr.NewInternalErrorf(context.Background(), "external write (jsonline): %v", err)
		}
		return nil
	case types.T_bool:
		if vector.GetFixedAtNoTypeCheck[bool](vec, i) {
			buf.Write(jsonTrue)
		} else {
			buf.Write(jsonFalse)
		}
		return nil
	case types.T_bit:
		// bit values are raw bytes; bytes >= 0x80 are invalid UTF-8 and cannot
		// round-trip through a JSON string. DDL rejects bit columns on writable
		// jsonline tables; this guards the unreachable path.
		return moerr.NewNotSupported(context.Background(),
			"external write (jsonline): bit column cannot round-trip through JSON")
	case types.T_int8:
		w.scratch = strconv.AppendInt(w.scratch[:0], int64(vector.GetFixedAtNoTypeCheck[int8](vec, i)), 10)
	case types.T_int16:
		w.scratch = strconv.AppendInt(w.scratch[:0], int64(vector.GetFixedAtNoTypeCheck[int16](vec, i)), 10)
	case types.T_int32:
		w.scratch = strconv.AppendInt(w.scratch[:0], int64(vector.GetFixedAtNoTypeCheck[int32](vec, i)), 10)
	case types.T_int64:
		w.scratch = strconv.AppendInt(w.scratch[:0], vector.GetFixedAtNoTypeCheck[int64](vec, i), 10)
	case types.T_uint8:
		w.scratch = strconv.AppendUint(w.scratch[:0], uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, i)), 10)
	case types.T_uint16:
		w.scratch = strconv.AppendUint(w.scratch[:0], uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, i)), 10)
	case types.T_uint32:
		w.scratch = strconv.AppendUint(w.scratch[:0], uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), 10)
	case types.T_uint64:
		w.scratch = strconv.AppendUint(w.scratch[:0], vector.GetFixedAtNoTypeCheck[uint64](vec, i), 10)
	case types.T_float32:
		return w.appendJSONFloat(buf, float64(vector.GetFixedAtNoTypeCheck[float32](vec, i)), 32)
	case types.T_float64:
		return w.appendJSONFloat(buf, vector.GetFixedAtNoTypeCheck[float64](vec, i), 64)
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		b := vec.GetBytesAt(i)
		// The jsonline reader compares every decoded string against the \N null
		// token (reader_csv.go JsonNull), so that exact content cannot
		// round-trip under any encoding; and its JSON tokenizer rewrites
		// invalid UTF-8 bytes to U+FFFD, mutating the value. Reject both
		// rather than corrupt silently.
		if bytes.Equal(b, csvNull) {
			return moerr.NewNotSupported(context.Background(),
				"external write (jsonline): a string value of exactly \\N reads back as NULL and cannot round-trip")
		}
		if !utf8.Valid(b) {
			return moerr.NewNotSupported(context.Background(),
				"external write (jsonline): string value contains invalid UTF-8, which the jsonline reader rewrites; use csv format for raw bytes")
		}
		appendJSONString(buf, b)
		return nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		// Binary payloads cannot round-trip: a base64 JSON string would be
		// appended verbatim by the jsonline READER (it does not decode), and raw
		// bytes are not valid JSON. DDL rejects these columns on writable
		// jsonline tables; this guards the unreachable path.
		return moerr.NewNotSupportedf(context.Background(),
			"external write (jsonline): %s column cannot round-trip through JSON", vec.GetType().Oid.String())
	case types.T_array_float32:
		return appendJSONFloatArray(w, buf, types.BytesToArray[float32](vec.GetBytesAt(i)), 32)
	case types.T_array_float64:
		return appendJSONFloatArray(w, buf, types.BytesToArray[float64](vec.GetBytesAt(i)), 64)
	case types.T_date:
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Date](vec, i).String()))
		return nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)))
		return nil
	case types.T_time:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)))
		return nil
	case types.T_timestamp:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(w.cfg.TimeZone, scale)))
		return nil
	case types.T_year:
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.MoYear](vec, i).String()))
		return nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)))
		return nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)))
		return nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale)))
		return nil
	case types.T_uuid:
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()))
		return nil
	case types.T_enum:
		appendJSONString(buf, []byte(vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()))
		return nil
	default:
		return moerr.NewInternalErrorf(context.Background(),
			"external write (jsonline): unsupported column type %s", vec.GetType().String())
	}
	buf.Write(w.scratch)
	return nil
}

// appendJSONFloat appends f formatted exactly as encoding/json formats floats
// (shortest 'f' form, switching to 'e' outside [1e-6, 1e21) with a trimmed
// exponent), so the rewrite keeps byte-identical output.
func (w *externalWriter) appendJSONFloat(buf *bytes.Buffer, f float64, bits int) error {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return moerr.NewInternalErrorf(context.Background(),
			"external write (jsonline): unsupported float value %v", f)
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) ||
			bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	w.scratch = strconv.AppendFloat(w.scratch[:0], f, format, -1, bits)
	if format == 'e' {
		// clean up e-09 to e-9, as encoding/json does
		if n := len(w.scratch); n >= 4 && w.scratch[n-4] == 'e' && w.scratch[n-3] == '-' && w.scratch[n-2] == '0' {
			w.scratch[n-2] = w.scratch[n-1]
			w.scratch = w.scratch[:n-1]
		}
	}
	buf.Write(w.scratch)
	return nil
}

func appendJSONFloatArray[T float32 | float64](w *externalWriter, buf *bytes.Buffer, vals []T, bits int) error {
	buf.WriteByte('[')
	for k, v := range vals {
		if k > 0 {
			buf.WriteByte(',')
		}
		if err := w.appendJSONFloat(buf, float64(v), bits); err != nil {
			return err
		}
	}
	buf.WriteByte(']')
	return nil
}

// appendJSONString writes s to buf as a JSON string, escaping quotes,
// backslashes and control characters. (Unlike encoding/json it does not
// HTML-escape & < >, which the reader's JSON parser does not require.
// Callers must reject invalid UTF-8 first: the reader's tokenizer rewrites
// such bytes to U+FFFD.)
func appendJSONString(buf *bytes.Buffer, s []byte) {
	const hexDigits = "0123456789abcdef"
	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		buf.Write(s[start:i])
		switch c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			buf.WriteString(`\u00`)
			buf.WriteByte(hexDigits[c>>4])
			buf.WriteByte(hexDigits[c&0xF])
		}
		start = i + 1
	}
	buf.Write(s[start:])
	buf.WriteByte('"')
}

// colCount is the number of leading batch columns to write: the table's
// declared columns (len(Attrs)). The execution pipeline may append trailing
// hidden vectors that must not be emitted.
func (w *externalWriter) colCount(bat *batch.Batch) int {
	n := len(w.cfg.Attrs)
	if n == 0 || n > len(bat.Vecs) {
		n = len(bat.Vecs)
	}
	return n
}

// addEscape doubles the escape character and the enclosure character so the
// reader's unescaping (E E -> E) and doubled-quote collapsing (Q Q -> Q)
// reproduce the original bytes, and rewrites 0x0D as E+'r' (the reader's
// MySQL unescaper restores it; left raw, a trailing CR would be eaten by the
// reader's end-of-record CR strip, which applies even to enclosed fields).
// escape == 0 means escaping is disabled (an empty FIELDS ESCAPED BY),
// enclosed == 0 means
// the value is written unenclosed. The common nothing-to-escape case returns
// s unchanged (no copy), preserving GetBytesAt's zero-copy slice.
func addEscape(s []byte, enclosed byte, escape byte) []byte {
	needEscape := escape != 0 && bytes.IndexByte(s, escape) >= 0
	needQuote := enclosed != 0 && enclosed != escape && bytes.IndexByte(s, enclosed) >= 0
	needCR := escape != 0 && bytes.IndexByte(s, '\r') >= 0
	if !needEscape && !needQuote && !needCR {
		return s
	}
	if needEscape {
		s = bytes.ReplaceAll(s, []byte{escape}, []byte{escape, escape})
	}
	if needQuote {
		s = bytes.ReplaceAll(s, []byte{enclosed}, []byte{enclosed, enclosed})
	}
	if needCR {
		s = bytes.ReplaceAll(s, []byte{'\r'}, []byte{escape, 'r'})
	}
	return s
}
