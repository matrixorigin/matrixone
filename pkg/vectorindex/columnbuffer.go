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

package vectorindex

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ColumnBuffer is a batch of primary keys in a TYPED, box-free columnar form, used by
// the fulltext2 no-LIMIT streaming path (RuntimeConfig.Emit). A pk column has ONE type,
// so every key in a batch is uniformly fixed-width or varlena and Type alone
// disambiguates Data:
//   - fixed-width type (int/uint/temporal/decimal): Data is N contiguous width-byte
//     little-endian values (no length prefix; width is implied by Type).
//   - varlena type (varchar/blob/json/uuid): Data is N [u32 len][content] entries.
//
// Neither form boxes into any, and Data is a copy (not a view into the segment mmap),
// so an in-flight batch survives segment eviction.
type ColumnBuffer struct {
	Type types.T
	Data []byte // fixed: N×width bytes; varlena: [u32 len][content] entries
	N    int    // element count
	// Nulls is the per-element SQL-NULL flag, used only by NULLABLE columns (fulltext2
	// INCLUDE cols). It stays nil for pk columns (a pk is never NULL) — a nil Nulls means
	// "all non-null", so the pk path is unchanged. When non-nil it is kept parallel to N
	// (len(Nulls) == N): a NULL element still carries a well-formed placeholder in Data (a
	// zero-filled fixed value / a [u32 0] varlena entry) so the Data cursor stays aligned.
	Nulls []bool
}

// Reset clears a ColumnBuffer for reuse without dropping its backing buffer. The
// streaming producer/consumer recycle these through fulltext2's pool (GetColumnBuffer /
// PutColumnBuffer); the pooling POLICY lives with its user, not in this shared struct.
func (k *ColumnBuffer) Reset() {
	k.Data = k.Data[:0]
	k.N = 0
	if k.Nulls != nil {
		k.Nulls = k.Nulls[:0]
	}
}

// AppendColumnBuffer decodes a whole ColumnBuffer batch straight into vec (see
// AppendColumnBufferRange for the semantics); it is AppendColumnBufferRange over [0, k.N).
// It is the generic inverse of "fill a ColumnBuffer" — it reads only ColumnBuffer's own
// documented Data/Nulls layout (not any algo's on-disk format), so it lives here with the
// type. The box-free ENCODE side (segment bytes → ColumnBuffer) is algo-specific and stays
// in the algo package (e.g. fulltext2's appendPkTo/appendIncludeTo).
func AppendColumnBuffer(k *ColumnBuffer, vec *vector.Vector, mp *mpool.MPool) error {
	return AppendColumnBufferRange(k, vec, 0, k.N, mp)
}

// AppendColumnBufferRange decodes the rows [start, start+n) of a ColumnBuffer straight into
// vec, appending each element with the TYPED vector.AppendFixed/AppendBytes — no per-element
// boxing on the consumer side (the mirror of the producer's box-free encode). The Data layout
// is: fixed-width types are N contiguous width-byte little-endian values; varlena types are N
// [u32 len][content] entries. vec's type MUST match k.Type.
//
// The range form lets the LIMIT-path TVF page one whole-result buffer in output-sized chunks
// (the streaming path emits a fresh per-batch buffer instead, so it uses the [0,N) wrapper).
// A fixed-width column seeks to start in O(1); a varlena column walks the length prefixes up
// to start (O(start)), which across P forward pages totals O(N·P) — negligible for a LIMIT.
//
// NULLs: a pk column leaves k.Nulls nil (never NULL) — nul(i) is always false, unchanged.
// A nullable INCLUDE column carries k.Nulls parallel to N; a NULL element still occupies a
// well-formed placeholder in Data (zero-filled fixed value / [u32 0] varlena) so the cursor
// stays aligned, and nul(i) makes AppendFixed/AppendBytes write a SQL NULL instead.
func AppendColumnBufferRange(k *ColumnBuffer, vec *vector.Vector, start, n int, mp *mpool.MPool) error {
	// Each arm loops with an `err == nil` guard and a single `return err` at the end, so
	// AppendFixed/AppendBytes's error path (which does not fire for a well-typed vector +
	// valid value) needs no per-arm return block.
	if start < 0 {
		start = 0
	}
	end := start + n
	if end > k.N {
		end = k.N
	}
	if start >= end {
		return nil
	}
	d := k.Data
	nul := func(i int) bool { return k.Nulls != nil && i < len(k.Nulls) && k.Nulls[i] }
	var err error
	switch k.Type {
	case types.T_int64:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, int64(binary.LittleEndian.Uint64(d[i*8:])), nul(i), mp)
		}
	case types.T_uint64, types.T_bit:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, binary.LittleEndian.Uint64(d[i*8:]), nul(i), mp)
		}
	case types.T_int32:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, int32(binary.LittleEndian.Uint32(d[i*4:])), nul(i), mp)
		}
	case types.T_uint32:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, binary.LittleEndian.Uint32(d[i*4:]), nul(i), mp)
		}
	case types.T_int16:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, int16(binary.LittleEndian.Uint16(d[i*2:])), nul(i), mp)
		}
	case types.T_uint16:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, binary.LittleEndian.Uint16(d[i*2:]), nul(i), mp)
		}
	case types.T_int8:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, int8(d[i]), nul(i), mp)
		}
	case types.T_uint8:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, d[i], nul(i), mp)
		}
	case types.T_date:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, types.Date(int32(binary.LittleEndian.Uint32(d[i*4:]))), nul(i), mp)
		}
	case types.T_datetime:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, types.Datetime(int64(binary.LittleEndian.Uint64(d[i*8:]))), nul(i), mp)
		}
	case types.T_time:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, types.Time(int64(binary.LittleEndian.Uint64(d[i*8:]))), nul(i), mp)
		}
	case types.T_timestamp:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, types.Timestamp(int64(binary.LittleEndian.Uint64(d[i*8:]))), nul(i), mp)
		}
	case types.T_decimal64:
		for i := start; err == nil && i < end; i++ {
			err = vector.AppendFixed(vec, types.Decimal64(binary.LittleEndian.Uint64(d[i*8:])), nul(i), mp)
		}
	case types.T_decimal128:
		for i := start; err == nil && i < end; i++ {
			b := d[i*16:]
			err = vector.AppendFixed(vec, types.Decimal128{B0_63: binary.LittleEndian.Uint64(b[0:8]), B64_127: binary.LittleEndian.Uint64(b[8:16])}, nul(i), mp)
		}
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		off := 0
		for i := 0; i < start; i++ { // walk length prefixes to the start row
			off += 4 + int(binary.LittleEndian.Uint32(d[off:]))
		}
		for i := start; err == nil && i < end; i++ {
			l := int(binary.LittleEndian.Uint32(d[off:]))
			off += 4
			err = vector.AppendBytes(vec, d[off:off+l], nul(i), mp)
			off += l
		}
	case types.T_uuid:
		off := 0
		for i := 0; i < start; i++ { // walk length prefixes to the start row
			off += 4 + int(binary.LittleEndian.Uint32(d[off:]))
		}
		for i := start; err == nil && i < end; i++ {
			l := int(binary.LittleEndian.Uint32(d[off:]))
			off += 4
			if nul(i) {
				err = vector.AppendFixed(vec, types.Uuid{}, true, mp)
			} else {
				var u types.Uuid
				if u, err = types.ParseUuid(string(d[off : off+l])); err == nil {
					err = vector.AppendFixed(vec, u, false, mp)
				}
			}
			off += l
		}
	default:
		return moerr.NewInternalErrorNoCtxf("column buffer: unsupported type %d", k.Type)
	}
	return err
}
