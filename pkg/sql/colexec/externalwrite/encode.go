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
	"encoding/base64"
	"encoding/json"
	"slices"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// encodeCSV renders every row of bat as a CSV record. The per-type formatting
// mirrors the SELECT INTO OUTFILE encoder (pkg/frontend/export.go constructByte)
// so the output round-trips through the external-table CSV reader.
func (w *externalWriter) encodeCSV(bat *batch.Batch) ([]byte, error) {
	buf := &bytes.Buffer{}
	enclosed := w.cfg.EnclosedBy
	// Only the table's columns are written; the pipeline may carry trailing
	// hidden vectors (mirrors insert_table, which copies only InsertCtx.Attrs).
	ncol := w.colCount(bat)

	for i := 0; i < bat.RowCount(); i++ {
		for j := 0; j < ncol; j++ {
			vec := bat.Vecs[j]
			last := j == ncol-1
			if cellIsNull(vec, i) {
				w.writeCSVField(buf, []byte("\\N"), false, last)
				continue
			}
			val, quote, err := w.csvValue(vec, i)
			if err != nil {
				return nil, err
			}
			if quote {
				val = addEscape(val, enclosed)
			}
			w.writeCSVField(buf, val, quote, last)
		}
	}
	return buf.Bytes(), nil
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
			return []byte("true"), false, nil
		}
		return []byte("false"), false, nil
	case types.T_bit:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
		bitLength := vec.GetType().Width
		byteLength := (bitLength + 7) / 8
		b := types.EncodeUint64(&v)[:byteLength]
		b = slices.Clone(b)
		slices.Reverse(b)
		return b, false, nil
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
		return []byte(vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()), false, nil
	default:
		return nil, false, moerr.NewInternalErrorf(context.Background(),
			"external write (csv): unsupported column type %s", vec.GetType().String())
	}
}

// encodeJSONLine renders every row of bat as a JSONLine record (one JSON object
// per line). Mirrors pkg/frontend/export.go constructJSONLine / vectorValueToJSON.
func (w *externalWriter) encodeJSONLine(bat *batch.Batch) ([]byte, error) {
	buf := &bytes.Buffer{}
	ncol := w.colCount(bat)
	for i := 0; i < bat.RowCount(); i++ {
		row := make(map[string]interface{}, ncol)
		for j := 0; j < ncol; j++ {
			vec := bat.Vecs[j]
			name := w.cfg.Attrs[j]
			if cellIsNull(vec, i) {
				row[name] = nil
				continue
			}
			v, err := w.jsonValue(vec, i)
			if err != nil {
				return nil, err
			}
			row[name] = v
		}
		jb, err := json.Marshal(row)
		if err != nil {
			return nil, moerr.NewInternalErrorf(context.Background(), "external write (jsonline): %v", err)
		}
		buf.Write(jb)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func (w *externalWriter) jsonValue(vec *vector.Vector, i int) (interface{}, error) {
	switch vec.GetType().Oid {
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		return json.RawMessage(val.String()), nil
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, i), nil
	case types.T_bit:
		// The external reader parses bit columns from their raw big-endian byte
		// representation (see external.go getColData T_bit), the same form the CSV
		// writer emits. Emit those bytes as a JSON string so the value round-trips;
		// a plain decimal number would be read back byte-by-byte and corrupt it.
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, i)
		byteLength := (vec.GetType().Width + 7) / 8
		b := slices.Clone(types.EncodeUint64(&v)[:byteLength])
		slices.Reverse(b)
		return string(b), nil
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, i), nil
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, i), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, i), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, i), nil
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, i), nil
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, i), nil
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, i), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, i), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, i), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, i), nil
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		return string(vec.GetBytesAt(i)), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return base64.StdEncoding.EncodeToString(vec.GetBytesAt(i)), nil
	case types.T_array_float32:
		return types.BytesToArray[float32](vec.GetBytesAt(i)), nil
	case types.T_array_float64:
		return types.BytesToArray[float64](vec.GetBytesAt(i)), nil
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, i).String(), nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale), nil
	case types.T_time:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale), nil
	case types.T_timestamp:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(w.cfg.TimeZone, scale), nil
	case types.T_year:
		return vector.GetFixedAtNoTypeCheck[types.MoYear](vec, i).String(), nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale), nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale), nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, i).Format(scale), nil
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String(), nil
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String(), nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(),
			"external write (jsonline): unsupported column type %s", vec.GetType().String())
	}
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

// cellIsNull reports whether row i of vec is NULL, handling constant and
// constant-null vectors (whose physical data lives at index 0, or is absent).
func cellIsNull(vec *vector.Vector, i int) bool {
	if vec.IsConstNull() {
		return true
	}
	idx := i
	if vec.IsConst() {
		idx = 0
	}
	return vec.GetNulls().Contains(uint64(idx))
}

// addEscape escapes backslashes and (doubled) the enclosure character, matching
// pkg/frontend/export.go addEscapeToString.
func addEscape(s []byte, escape byte) []byte {
	s = bytes.ReplaceAll(s, []byte{'\\'}, []byte{'\\', '\\'})
	if escape != 0 && escape != '\\' {
		s = bytes.ReplaceAll(s, []byte{escape}, []byte{escape, escape})
	}
	return s
}
