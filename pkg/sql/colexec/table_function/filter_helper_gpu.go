//go:build gpu

// Copyright 2022 Matrix Origin
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

package table_function

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
)

// filterColumnBuilder is satisfied by the index builders in
// pkg/vectorindex/{cagra,ivfpq} — both expose the same two-method filter API.
// The table function uses this narrow interface so the helpers stay generic
// across CAGRA / IVF-PQ.
//
// AddFilterChunk's nullBitmap follows MO null-mask semantics: LSB-first,
// bit i = 1 means row i IS NULL. The C++ side (FilterStore::add_chunk) inverts
// into its internal validity array (bit=1 = not-null) at the boundary.
type filterColumnBuilder interface {
	SetFilterColumns(colMetaJSON string)
	AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error
}

// Null bitmap for a 1-row chunk where the single row is null: bit 0 = 1.
// Shared so the hot path doesn't allocate per call; C++ copies bits out
// before returning.
var nullBitmapOneRowIsNull = []uint32{1}

// initFilterColumns serialises the derived []cuvsfilter.ColumnMeta into the
// JSON shape accepted by gpu_<idx>_set_filter_columns and registers it on
// the builder. A no-op when cols is empty. Call once in start() after the
// builder is constructed.
func initFilterColumns(build filterColumnBuilder, cols []cuvsfilter.ColumnMeta) error {
	if len(cols) == 0 {
		return nil
	}
	buf, err := sonic.Marshal(cols)
	if err != nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("marshal filter columns: %v", err))
	}
	build.SetFilterColumns(string(buf))
	return nil
}

// appendFilterRow reads one row of filter-column values from argVecs and
// forwards the raw bytes to the builder. argOffset is the index of the first
// filter-column arg in argVecs (3 for both cagra_create and ivfpq_create —
// tblcfg, pk, vec, then filter cols).
//
// The raw-bytes contract matches the C++ side: row-major, elem_size bytes per
// value, native byte order (LE on x86_64). For numeric types we slice the
// cell directly via unsafe.Pointer rather than re-encode.
//
// VARCHAR (FilterColTypeUint64) is NOT supported at this layer — the DDL
// layer is expected to have emitted a precomputed hash column. Leaving the
// hashing responsibility there keeps this helper agnostic of string semantics.
func appendFilterRow(
	build filterColumnBuilder,
	cols []cuvsfilter.ColumnMeta,
	argVecs []*vector.Vector,
	argOffset int,
	nthRow int,
) error {
	for i, meta := range cols {
		v := argVecs[argOffset+i]
		// Per-row append. Under MO's null-mask contract (bit=1 means NULL),
		// a null row passes a 1-word bitmap with bit 0 = 1. A non-null row
		// passes nil, letting the C++ side keep the column dense (no validity
		// allocation) — this is the fast path worth preserving, since MO
		// vectors are typically dense.
		//
		// For null rows the byte payload is undefined (MO's fixed-width array
		// holds whatever pattern was left at that slot). That's fine: the
		// validity AND in eval_filter_bitmap_cpu masks the comparison result
		// to 0 regardless of the payload bytes.
		var nullBitmap []uint32
		if v.IsNull(uint64(nthRow)) {
			nullBitmap = nullBitmapOneRowIsNull
		}
		switch meta.TypeOid {
		case cuvsfilter.ColTypeInt32:
			val := vector.GetFixedAtNoTypeCheck[int32](v, nthRow)
			buf := (*[4]byte)(unsafe.Pointer(&val))[:]
			if err := build.AddFilterChunk(uint32(i), buf, nullBitmap, 1); err != nil {
				return err
			}
		case cuvsfilter.ColTypeInt64:
			val := vector.GetFixedAtNoTypeCheck[int64](v, nthRow)
			buf := (*[8]byte)(unsafe.Pointer(&val))[:]
			if err := build.AddFilterChunk(uint32(i), buf, nullBitmap, 1); err != nil {
				return err
			}
		case cuvsfilter.ColTypeFloat32:
			val := vector.GetFixedAtNoTypeCheck[float32](v, nthRow)
			buf := (*[4]byte)(unsafe.Pointer(&val))[:]
			if err := build.AddFilterChunk(uint32(i), buf, nullBitmap, 1); err != nil {
				return err
			}
		case cuvsfilter.ColTypeFloat64:
			val := vector.GetFixedAtNoTypeCheck[float64](v, nthRow)
			buf := (*[8]byte)(unsafe.Pointer(&val))[:]
			if err := build.AddFilterChunk(uint32(i), buf, nullBitmap, 1); err != nil {
				return err
			}
		case cuvsfilter.ColTypeUint64:
			val := vector.GetFixedAtNoTypeCheck[uint64](v, nthRow)
			buf := (*[8]byte)(unsafe.Pointer(&val))[:]
			if err := build.AddFilterChunk(uint32(i), buf, nullBitmap, 1); err != nil {
				return err
			}
		default:
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unsupported filter column type: %d", meta.TypeOid))
		}
	}
	return nil
}

// validateFilterArgCount checks that tf.ctr.argVecs has enough entries for
// the base args + declared filter columns. Deeper type matching is left to
// appendFilterRow (the DDL layer is authoritative).
func validateFilterArgCount(argVecs []*vector.Vector, baseArgCount int, cols []cuvsfilter.ColumnMeta) error {
	if len(argVecs) < baseArgCount+len(cols) {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"filter args mismatch: have %d args, need %d (%d base + %d filter columns)",
			len(argVecs), baseArgCount+len(cols), baseArgCount, len(cols)))
	}
	return nil
}

// parseIncludedColumnNames splits the comma-joined "included_columns" entry
// from an index's params JSON (CagraParam.IncludedColumns /
// IvfpqParam.IncludedColumns). Empty input produces nil — callers treat that
// as "no INCLUDE columns declared" and skip filter setup entirely.
func parseIncludedColumnNames(joined string) []string {
	if joined == "" {
		return nil
	}
	raw := strings.Split(joined, ",")
	out := make([]string, 0, len(raw))
	for _, n := range raw {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

// buildFilterColumnsFromParam pairs the INCLUDE column names from the params
// JSON with the types of the corresponding argVecs to produce the full
// []cuvsfilter.ColumnMeta expected by the C++ FilterStore. argOffset is the
// index of the first filter-column arg (3 for both cagra_create and
// ivfpq_create — tblcfg, pk, vec, then filter cols).
//
// Returns (nil, nil) when the index has no INCLUDE columns declared; callers
// should treat that as "skip all filter setup". Returns an error when the
// number of arg vectors is insufficient or any vector's type isn't supported
// by FilterStore.
func buildFilterColumnsFromParam(
	includedNames string,
	argVecs []*vector.Vector,
	argOffset int,
) ([]cuvsfilter.ColumnMeta, error) {
	names := parseIncludedColumnNames(includedNames)
	if len(names) == 0 {
		return nil, nil
	}
	if len(argVecs) < argOffset+len(names) {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"filter args mismatch: have %d args, need %d (%d base + %d INCLUDE columns from params)",
			len(argVecs), argOffset+len(names), argOffset, len(names)))
	}
	cols := make([]cuvsfilter.ColumnMeta, len(names))
	for i, name := range names {
		oid := argVecs[argOffset+i].GetType().Oid
		ct, err := filterColTypeFromOid(oid)
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"INCLUDE column '%s' has unsupported type %s", name, oid.String()))
		}
		cols[i] = cuvsfilter.ColumnMeta{Name: name, TypeOid: ct}
	}
	return cols, nil
}

// filterColTypeFromOid maps a MO types.T onto the narrow physical set the
// C++ FilterStore understands. T_uint64 is reserved for VARCHAR/char columns
// the DDL layer has already FNV-hashed before reaching the table function.
func filterColTypeFromOid(t types.T) (cuvsfilter.ColType, error) {
	switch t {
	case types.T_int32:
		return cuvsfilter.ColTypeInt32, nil
	case types.T_int64:
		return cuvsfilter.ColTypeInt64, nil
	case types.T_float32:
		return cuvsfilter.ColTypeFloat32, nil
	case types.T_float64:
		return cuvsfilter.ColTypeFloat64, nil
	case types.T_uint64:
		return cuvsfilter.ColTypeUint64, nil
	}
	return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("unsupported INCLUDE column type %s", t.String()))
}
