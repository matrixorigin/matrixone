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

package iscp

import (
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// newTestCuvsTableDef builds a minimal TableDef for a cuvs-backed
// index (CAGRA / IVF-PQ shape: bigint PK, vecf32 vector column, two
// hidden tables — metadata + storage). Optionally adds INCLUDE
// columns (added after the PK and vector, in order).
func newTestCuvsTableDef(pkName, vecColName string, vecWidth int32, includeCols ...includeColSpec) *plan.TableDef {
	name2col := map[string]int32{
		pkName:     0,
		vecColName: 1,
	}
	cols := []*plan.ColDef{
		{Name: pkName, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: vecColName, Typ: plan.Type{Id: int32(types.T_array_float32), Width: vecWidth}},
	}
	for i, ic := range includeCols {
		idx := int32(2 + i)
		name2col[ic.name] = idx
		cols = append(cols, &plan.ColDef{Name: ic.name, Typ: plan.Type{Id: int32(ic.typ)}})
	}

	algoParams := ""
	if len(includeCols) > 0 {
		names := make([]string, len(includeCols))
		for i, ic := range includeCols {
			names[i] = ic.name
		}
		algoParams = `{"included_columns":"` + strings.Join(names, ",") + `"}`
	}

	idx := func(tblType, tblName string) *plan.IndexDef {
		return &plan.IndexDef{
			IndexName:          "cuvs_idx",
			TableExist:         true,
			IndexAlgo:          catalog.MoIndexCagraAlgo.ToString(), // diagnostic; writer doesn't switch on it
			IndexAlgoTableType: tblType,
			IndexTableName:     tblName,
			Parts:              []string{vecColName},
			IndexAlgoParams:    algoParams,
		}
	}

	return &plan.TableDef{
		Name:          "test_orig_tbl",
		Name2ColIndex: name2col,
		Cols:          cols,
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			idx(catalog.Cagra_TblType_Metadata, "meta_tbl"),
			idx(catalog.Cagra_TblType_Storage, "storage_tbl"),
		},
	}
}

type includeColSpec struct {
	name string
	typ  types.T
}

func newTestCuvsConsumerInfo() *ConsumerInfo {
	return &ConsumerInfo{
		ConsumerType: 0,
		DBName:       "test_db",
		TableName:    "test_tbl",
		IndexName:    "cuvs_idx",
	}
}

func newTestCuvsIndexDefs(td *plan.TableDef) []*plan.IndexDef {
	// Pass both hidden-table indexdefs (writer requires exactly 2).
	out := make([]*plan.IndexDef, 0, 2)
	for _, ix := range td.Indexes {
		out = append(out, ix)
	}
	return out
}

// ---------------------------------------------------------------------------
// Constructor validation
// ---------------------------------------------------------------------------

func TestNewCuvsCdcWriter_Success(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	w, err := NewCuvsCdcWriter("cagra", "test_db", "test_tbl", "cuvs_idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)
	require.Equal(t, int32(4), w.Dimension())
	require.Equal(t, "test_db", w.DbName())
	require.Equal(t, "test_tbl", w.TblName())
	require.Equal(t, "cuvs_idx", w.IndexName())
	require.Empty(t, w.ColMetaJSON(), "no INCLUDE cols → empty colMetaJSON")
}

func TestNewCuvsCdcWriter_RejectsMultiPK(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	td.Pkey.Names = []string{"pk", "pk2"}
	_, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.Error(t, err)
	require.Contains(t, err.Error(), "one primary key")
}

func TestNewCuvsCdcWriter_RejectsWrongIndexCount(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	_, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, td.Indexes[:1])
	require.Error(t, err)
	require.Contains(t, err.Error(), "2 secondary tables")
}

func TestNewCuvsCdcWriter_RejectsMultiPartIndex(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	// Corrupt the first hidden-table indexdef to have 2 parts.
	td.Indexes[0].Parts = []string{"v", "extra"}
	_, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.Error(t, err)
	require.Contains(t, err.Error(), "one vector part")
}

func TestNewCuvsCdcWriter_RejectsNonBigintPK(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	td.Cols[0].Typ.Id = int32(types.T_int32) // not bigint
	_, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.Error(t, err)
	require.Contains(t, err.Error(), "bigint")
}

func TestNewCuvsCdcWriter_RejectsVecF64(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 4)
	td.Cols[1].Typ.Id = int32(types.T_array_float64)
	_, err := NewCuvsCdcWriter("ivfpq", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.Error(t, err)
	require.Contains(t, err.Error(), "fp32-only")
}

// ---------------------------------------------------------------------------
// Insert / Upsert / Delete encoding
// ---------------------------------------------------------------------------

func TestCuvsCdcWriter_InsertEncodesAsInsertRecord(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 3)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)
	require.True(t, w.Empty())

	ctx := context.Background()
	row := []any{int64(42), []float32{1.5, -2.0, 3.0}}
	require.NoError(t, w.Insert(ctx, row))
	require.False(t, w.Empty())

	out, err := w.ToSql()
	require.NoError(t, err)

	// One INSERT record: 1 op + 8 pkid + 4*dim = 21 bytes.
	require.Len(t, out, 1+8+4*3)
	require.Equal(t, byte(vectorindex.CdcOpInsert), out[0])
	require.Equal(t, int64(42), int64(binary.LittleEndian.Uint64(out[1:9])))
	require.Equal(t, float32(1.5), math.Float32frombits(binary.LittleEndian.Uint32(out[9:13])))
}

func TestCuvsCdcWriter_UpsertEncodesAsInsertRecord(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	ctx := context.Background()
	row := []any{int64(100), []float32{0.5, 0.5}}
	require.NoError(t, w.Upsert(ctx, row))

	out, err := w.ToSql()
	require.NoError(t, err)
	require.Equal(t, byte(vectorindex.CdcOpInsert), out[0],
		"Upsert must encode as INSERT (last-write-wins via replay)")
}

func TestCuvsCdcWriter_DeleteEncodesAsDeleteRecord(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 3)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	// Delete row carries only the PK in row[0].
	require.NoError(t, w.Delete(context.Background(), []any{int64(99)}))

	out, err := w.ToSql()
	require.NoError(t, err)
	// One DELETE record: 1 op + 8 pkid = 9 bytes.
	require.Len(t, out, 9)
	require.Equal(t, byte(vectorindex.CdcOpDelete), out[0])
	require.Equal(t, int64(99), int64(binary.LittleEndian.Uint64(out[1:9])))
}

func TestCuvsCdcWriter_InsertWithNilVectorEncodesAsDelete(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 3)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	// NULL vector → source row no longer has a vector to index → DELETE.
	require.NoError(t, w.Insert(context.Background(), []any{int64(7), nil}))
	out, err := w.ToSql()
	require.NoError(t, err)
	require.Equal(t, byte(vectorindex.CdcOpDelete), out[0])
}

func TestCuvsCdcWriter_MultipleEventsConcatenate(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, w.Insert(ctx, []any{int64(1), []float32{1, 1}}))
	require.NoError(t, w.Upsert(ctx, []any{int64(2), []float32{2, 2}}))
	require.NoError(t, w.Delete(ctx, []any{int64(3)}))

	out, err := w.ToSql()
	require.NoError(t, err)
	// 2 INSERT (1+8+4*2 each) + 1 DELETE (9) = 2*17 + 9 = 43
	require.Len(t, out, 2*(1+8+4*2)+9)
	// Sanity-check record boundaries: parse forward.
	require.Equal(t, byte(vectorindex.CdcOpInsert), out[0])
	require.Equal(t, byte(vectorindex.CdcOpInsert), out[17])
	require.Equal(t, byte(vectorindex.CdcOpDelete), out[34])
}

func TestCuvsCdcWriter_InvalidPKTypePanics(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)
	// PK is a string instead of int64.
	err = w.Insert(context.Background(), []any{"not-int64", []float32{1, 2}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected int64")
}

// ---------------------------------------------------------------------------
// INCLUDE columns
// ---------------------------------------------------------------------------

func TestCuvsCdcWriter_WithIncludeColumns(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2,
		includeColSpec{name: "category", typ: types.T_int64},
		includeColSpec{name: "score", typ: types.T_float32},
	)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)
	require.NotEmpty(t, w.ColMetaJSON(), "INCLUDE cols → non-empty colMetaJSON")
	require.Contains(t, w.ColMetaJSON(), `"category"`)
	require.Contains(t, w.ColMetaJSON(), `"score"`)

	// row layout: pk, vec, category, score.
	row := []any{int64(1), []float32{1.0, 1.0}, int64(42), float32(0.75)}
	require.NoError(t, w.Insert(context.Background(), row))

	out, err := w.ToSql()
	require.NoError(t, err)

	// Record: 1 op + 8 pkid + 4*dim + (8 + 4 + 1 mask) include = 30 bytes.
	expectedLen := 1 + 8 + 4*2 + 8 + 4 + 1
	require.Len(t, out, expectedLen)
	require.Equal(t, byte(vectorindex.CdcOpInsert), out[0])

	// Include bytes start after op(1) + pk(8) + vec(8) = 17.
	incOff := 1 + 8 + 4*2
	// category (int64) first.
	require.Equal(t, int64(42), int64(binary.LittleEndian.Uint64(out[incOff:incOff+8])))
	// score (float32) next.
	require.Equal(t, float32(0.75), math.Float32frombits(binary.LittleEndian.Uint32(out[incOff+8:incOff+12])))
	// null mask: all non-null → 0.
	require.Equal(t, byte(0x00), out[incOff+12])
}

func TestCuvsCdcWriter_IncludeColumnNullSetsMask(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2,
		includeColSpec{name: "category", typ: types.T_int64},
	)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	row := []any{int64(1), []float32{1, 2}, nil}
	require.NoError(t, w.Insert(context.Background(), row))

	out, _ := w.ToSql()
	// Last byte is the null mask. bit 0 set => 0x01.
	require.Equal(t, byte(0x01), out[len(out)-1])
}

// ---------------------------------------------------------------------------
// Reset / Full / Empty / ToSql semantics
// ---------------------------------------------------------------------------

func TestCuvsCdcWriter_ResetClearsBuffer(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	require.NoError(t, w.Insert(context.Background(), []any{int64(1), []float32{1, 2}}))
	require.False(t, w.Empty())

	w.Reset()
	require.True(t, w.Empty())
	out, _ := w.ToSql()
	require.Empty(t, out)
}

func TestCuvsCdcWriter_ToSqlCopiesBuffer(t *testing.T) {
	// Reset() after ToSql must not invalidate the returned slice.
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)

	require.NoError(t, w.Insert(context.Background(), []any{int64(1), []float32{1, 2}}))
	snapshot, err := w.ToSql()
	require.NoError(t, err)
	originalFirst := snapshot[0]

	w.Reset()
	require.NoError(t, w.Insert(context.Background(), []any{int64(99), []float32{9, 9}}))

	// snapshot must still reflect the first insert (pkid=1).
	require.Equal(t, originalFirst, snapshot[0])
	require.Equal(t, int64(1), int64(binary.LittleEndian.Uint64(snapshot[1:9])))
}

func TestCuvsCdcWriter_FullReportsCapacity(t *testing.T) {
	// Easy way to exercise Full(): a tiny vector and many inserts.
	// The default capacity is 8 MiB; one insert is ~21 bytes for dim=3.
	// We won't actually fill 8 MiB in a unit test — just verify the
	// boundary condition for an empty writer.
	td := newTestCuvsTableDef("pk", "v", 3)
	w, _ := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.False(t, w.Full(), "empty writer should not report full")
}

func TestCuvsCdcWriter_CheckLastOpAlwaysTrue(t *testing.T) {
	// CuvsCdcWriter doesn't constrain op transitions (unlike
	// fulltext/ivfflat which gate UPSERT-after-DELETE etc.). The
	// append-only event log handles all ordering on the sync side.
	td := newTestCuvsTableDef("pk", "v", 2)
	w, _ := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.True(t, w.CheckLastOp("any"))
	require.True(t, w.CheckLastOp(""))
}

// ---------------------------------------------------------------------------
// IndexSqlWriter interface satisfaction (compile-time check is in
// cuvs_writer.go; this is a runtime sanity test).
// ---------------------------------------------------------------------------

func TestCuvsCdcWriter_SatisfiesIndexSqlWriter(t *testing.T) {
	td := newTestCuvsTableDef("pk", "v", 2)
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "idx", td, newTestCuvsIndexDefs(td))
	require.NoError(t, err)
	var _ IndexSqlWriter = w
}
