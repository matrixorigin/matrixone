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

package fulltext2

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// pk codec — the SINGLE SOURCE OF TRUTH for how a primary key of a given types.T is
// stored and moved through fulltext2. It is little-endian and used by the docmap,
// CDC log, delete frames, and the streaming ColumnBuffer.
//
// ADDING A PK TYPE: every switch in this file must gain the new case together —
//   fixedPkByteWidth   (width, or false for varlena)
//   encodePk           (concrete value → content bytes)
//   decodePk           (content bytes → boxed value)
//   encodePkLen        (has fast cases; falls back to encodePk otherwise)
//   AppendColumnBuffer (content bytes → typed vector, box-free)
// TestPkCodecAllTypesRoundTrip loops every supported type through the codec so a
// missing case fails loudly rather than shipping (a missing T_bit case once stalled
// CDC — see the BIT-pk fix).
//
// NOTE: byte-for-byte identical to pkg/bm25/wand's pk codec — both engines store
// any-typed source PKs the same way. Extract to a shared pk-codec package once bm25
// and fulltext2 live in the same tree (rule of three).

// fixedPkByteWidth returns the exact stored byte width of a fixed-width pk type, and
// false for variable-width types (varchar/blob/uuid/json…). Used to validate a docmap's
// per-pk length prefix at load and to guard decodePk's fixed-width reads.
func fixedPkByteWidth(pkType int32) (int, bool) {
	switch types.T(pkType) {
	case types.T_int8, types.T_uint8:
		return 1, true
	case types.T_int16, types.T_uint16:
		return 2, true
	case types.T_int32, types.T_uint32, types.T_date:
		return 4, true
	case types.T_int64, types.T_uint64, types.T_bit,
		types.T_datetime, types.T_time, types.T_timestamp, types.T_decimal64:
		return 8, true
	case types.T_decimal128:
		return 16, true
	default:
		return 0, false
	}
}

func encodePk(pkType int32, v any) ([]byte, error) {
	switch types.T(pkType) {
	case types.T_int64:
		return packUint64(uint64(v.(int64))), nil
	case types.T_uint64:
		return packUint64(v.(uint64)), nil
	case types.T_bit:
		// BIT is backed by uint64 (types.MustFixedColWithTypeCheck[uint64]); encode
		// like T_uint64. build_ddl accepts a BIT primary key, so the codec must too —
		// otherwise the first CDC flush errors and stalls index maintenance.
		return packUint64(v.(uint64)), nil
	case types.T_int32:
		return packUint32(uint32(v.(int32))), nil
	case types.T_uint32:
		return packUint32(v.(uint32)), nil
	case types.T_int16:
		return packUint16(uint16(v.(int16))), nil
	case types.T_uint16:
		return packUint16(v.(uint16)), nil
	case types.T_int8:
		return []byte{byte(v.(int8))}, nil
	case types.T_uint8:
		return []byte{v.(uint8)}, nil
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return asBytes(v), nil
	case types.T_uuid:
		switch x := v.(type) {
		case string:
			return []byte(x), nil
		case types.Uuid:
			return []byte(x.String()), nil
		case []byte:
			return append([]byte(nil), x...), nil
		default:
			return nil, moerr.NewInternalErrorNoCtxf("fulltext2: uuid pk unexpected go type %T", v)
		}
	case types.T_date:
		return packUint32(uint32(int32(v.(types.Date)))), nil
	case types.T_datetime:
		return packUint64(uint64(int64(v.(types.Datetime)))), nil
	case types.T_time:
		return packUint64(uint64(int64(v.(types.Time)))), nil
	case types.T_timestamp:
		return packUint64(uint64(int64(v.(types.Timestamp)))), nil
	case types.T_decimal64:
		return packUint64(uint64(v.(types.Decimal64))), nil
	case types.T_decimal128:
		d := v.(types.Decimal128)
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:8], d.B0_63)
		binary.LittleEndian.PutUint64(b[8:16], d.B64_127)
		return b, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("fulltext2: unsupported pk type %d", pkType)
	}
}

func decodePk(pkType int32, b []byte) (any, error) {
	// Fixed-width types index into b with LittleEndian.Uint{16,32,64}, which panic on
	// a short slice. A corrupt docmap can store a wrong length prefix, so verify the
	// slice is wide enough and return a clean error instead of crashing the query
	// goroutine. (decodeDocmap also width-validates at load; this is belt-and-suspenders.)
	if w, fixed := fixedPkByteWidth(pkType); fixed && len(b) < w {
		return nil, moerr.NewInternalErrorNoCtxf("fulltext2: corrupt pk: type %d needs %d bytes, got %d", pkType, w, len(b))
	}
	switch types.T(pkType) {
	case types.T_int64:
		return int64(binary.LittleEndian.Uint64(b)), nil
	case types.T_uint64:
		return binary.LittleEndian.Uint64(b), nil
	case types.T_bit:
		return binary.LittleEndian.Uint64(b), nil
	case types.T_int32:
		return int32(binary.LittleEndian.Uint32(b)), nil
	case types.T_uint32:
		return binary.LittleEndian.Uint32(b), nil
	case types.T_int16:
		return int16(binary.LittleEndian.Uint16(b)), nil
	case types.T_uint16:
		return binary.LittleEndian.Uint16(b), nil
	case types.T_int8:
		return int8(b[0]), nil
	case types.T_uint8:
		return b[0], nil
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return append([]byte(nil), b...), nil
	case types.T_uuid:
		u, err := types.ParseUuid(string(b))
		if err != nil {
			return nil, err
		}
		return u, nil
	case types.T_date:
		return types.Date(int32(binary.LittleEndian.Uint32(b))), nil
	case types.T_datetime:
		return types.Datetime(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_time:
		return types.Time(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_timestamp:
		return types.Timestamp(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_decimal64:
		return types.Decimal64(binary.LittleEndian.Uint64(b)), nil
	case types.T_decimal128:
		return types.Decimal128{
			B0_63:   binary.LittleEndian.Uint64(b[0:8]),
			B64_127: binary.LittleEndian.Uint64(b[8:16]),
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("fulltext2: unsupported pk type %d", pkType)
	}
}

// encodePkLen writes a length-prefixed pk ([u32 len][content]) into the docmap buffer,
// deriving the content from encodePk (one codec switch, not a duplicate here).
func (w *leBuf) encodePkLen(pkType int32, v any) error {
	pkb, err := encodePk(pkType, v)
	if err != nil {
		return err
	}
	w.u32(uint32(len(pkb)))
	w.b.Write(pkb)
	return nil
}

// appendAnyPk re-encodes a boxed pk value into a ColumnBuffer's Data (used by the
// build-side / materialized-fallback streaming paths): fixed-width types contribute
// their width bytes, varlena types a [u32 len][content] entry. No NEW box.
func appendAnyPk(k *vectorindex.ColumnBuffer, fixedW int, v any) {
	content, err := encodePk(int32(k.Type), v)
	if err != nil {
		return // unsupported pk type never reaches streaming (validated at build)
	}
	if fixedW > 0 {
		k.Data = append(k.Data, content...) // exactly fixedW bytes
	} else {
		var lp [4]byte
		binary.LittleEndian.PutUint32(lp[:], uint32(len(content)))
		k.Data = append(k.Data, lp[:]...)
		k.Data = append(k.Data, content...)
	}
	k.N++
}

func packUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func packUint32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func packUint16(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

func asBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}
