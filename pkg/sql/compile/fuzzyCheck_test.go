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

package compile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestNewFuzzyCheckAttrUsesParentUniqueColName verifies that when a hidden
// unique-index table dispatches fuzzy check via Fuzzymessage.ParentUniqueCols,
// the duplicate-entry error reports the original user-visible column name
// (e.g. "a") in displayAttr while attr stays as the hidden index column so the
// background dedup SQL keeps hitting a real column of the hidden table.
func TestNewFuzzyCheckAttrUsesParentUniqueColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_a_index",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName, // "__mo_index_idx_col"
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "decimal15",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, catalog.IndexTableIndexColName, f.attr,
		"attr should stay as the hidden index column name so backgroundSQLCheck queries a real column")
	require.Equal(t, "a", f.displayAttr,
		"displayAttr should be the user-visible column name used in duplicate-entry errors")
	require.NotNil(t, f.col)
	require.Equal(t, "a", f.col.Name)
}

// TestNewFuzzyCheckAttrFallsBackToPkeyColName verifies the non-hidden-index
// insertion path still uses the primary-key column name for both the SQL and
// the error message.
func TestNewFuzzyCheckAttrFallsBackToPkeyColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, "id", f.attr)
	require.Equal(t, "id", f.displayAttr)
}

// TestNewFuzzyCheckCompositeUniqueIndexDisplayAttr verifies that when the
// hidden unique-index plan node carries a multi-column ParentUniqueCols,
// displayAttr is set to a "(c1,c2)" tuple so the duplicate-entry error
// reports a user-recognizable key name instead of __mo_index_idx_col.
func TestNewFuzzyCheckCompositeUniqueIndexDisplayAttr(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "t1",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "col1", Typ: plan.Type{}},
				{Name: "col2", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.isCompound)
	require.Equal(t, "(col1,col2)", f.displayAttr,
		"composite hidden unique must surface a user-visible tuple key, not the internal hidden column")
}

// TestNewFuzzyCheckCompoundKeyPath covers the compound primary key path —
// when the hidden PkeyColName is CPrimaryKeyColName, the helper must flip
// isCompound and populate compoundCols from Pkey.Names.
func TestNewFuzzyCheckCompoundKeyPath(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"a", "b"},
			},
			Cols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{}},
				{Name: "b", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.isCompound)
	require.Len(t, f.compoundCols, 2)
	require.Equal(t, "a", f.compoundCols[0].Name)
	require.Equal(t, "b", f.compoundCols[1].Name)
	// Real composite PK (not a hidden unique index): displayAttr must
	// surface the user-declared column list, not the internal
	// __mo_cpkey_col placeholder — DuplicateEntry runs through
	// firstlyCheck/backgroundSQLCheck which use displayAttr verbatim.
	require.NotEqual(t, catalog.CPrimaryKeyColName, f.displayAttr,
		"compound PK must not leak __mo_cpkey_col into duplicate-entry errors")
	require.Equal(t, "(a,b)", f.displayAttr)
}

// TestNewFuzzyCheckHiddenOnlyPath covers the "ALTER TABLE add unique index on
// existing table" shortcut: hidden index table with no Fuzzymessage ->
// onlyInsertHidden should be true.
func TestNewFuzzyCheckHiddenOnlyPath(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: catalog.PrefixIndexTableName + "unique_hash",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.onlyInsertHidden)
	// Hidden-only fallback must not leak the internal PK column name —
	// firstlyCheck / backgroundSQLCheck use displayAttr directly in
	// NewDuplicateEntry without going through RewriteHiddenIndexDupEntry.
	require.NotEqual(t, catalog.IndexTableIndexColName, f.displayAttr,
		"hidden-only fallback must not surface the internal index column name")
	require.NotContains(t, f.displayAttr, "__mo_",
		"hidden-only fallback must not surface any __mo_ internal name")
}

// Composite CREATE UNIQUE INDEX on an existing table hits the same
// onlyInsertHidden fallback, but the hidden table's PK is __mo_cpkey_col.
// That name must also not leak into the user-visible error.
func TestNewFuzzyCheckHiddenOnlyCompositeDoesNotLeakInternalName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: catalog.PrefixIndexTableName + "unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"a", "b"},
			},
			Cols: []*plan.ColDef{
				{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{}},
				{Name: "a", Typ: plan.Type{}},
				{Name: "b", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.onlyInsertHidden)
	require.NotEqual(t, catalog.CPrimaryKeyColName, f.displayAttr)
	require.NotContains(t, f.displayAttr, "__mo_")
}

// TestNewFuzzyCheckRejectsEmptyNames covers the error branch when tblName or
// dbName is missing.
func TestNewFuzzyCheckRejectsEmptyNames(t *testing.T) {
	n := &plan.Node{
		ObjRef:   &plan.ObjectRef{SchemaName: ""},
		TableDef: &plan.TableDef{Name: ""},
	}
	_, err := newFuzzyCheck(n)
	require.Error(t, err)
}

// TestFuzzyCheckResetAndClear exercises the lifecycle helpers so the reuse
// pool path is also covered.
func TestFuzzyCheckResetAndClear(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{}}},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	f.condition = "x=1"
	f.cnt = 3
	f.reset()
	require.Equal(t, "", f.condition)
	require.Equal(t, 0, f.cnt)
	require.Equal(t, "id", f.attr)

	f.clear()
	require.Equal(t, "", f.attr)
	require.Equal(t, "", f.displayAttr)
	require.Equal(t, "", f.db)
	require.Equal(t, "", f.tbl)
	require.False(t, f.isCompound)
	require.False(t, f.onlyInsertHidden)
	require.Nil(t, f.col)
	require.Nil(t, f.compoundCols)

	// TypeName returns a stable identifier used by the reuse pool.
	require.Equal(t, "compile.fuzzyCheck", f.TypeName())
}

// TestConstructFuzzyFilterUsesParentUniqueColName verifies the fuzzy filter
// operator carries the user-visible column name for unique-index hidden tables
// so runtime duplicate errors report "for key 'a'" not "for key
// '__mo_index_idx_col'".
func TestConstructFuzzyFilterUsesParentUniqueColName(t *testing.T) {
	idxColType := plan.Type{Id: 27}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_a_index",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: idxColType},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "decimal15",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "a", Typ: idxColType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "a", op.PkName)
}

// TestConstructFuzzyFilterFallsBackToPkeyColName verifies non-hidden-index
// targets keep using the primary-key column name.
func TestConstructFuzzyFilterFallsBackToPkeyColName(t *testing.T) {
	pkType := plan.Type{Id: 23}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: pkType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "id", op.PkName)
}

// When plan has no Fuzzymessage and the target is a hidden index table
// (CREATE UNIQUE INDEX direct-insert path), constructFuzzyFilter must
// substitute a neutral placeholder for PkName instead of the raw
// __mo_index_idx_col / __mo_cpkey_col column name.
func TestConstructFuzzyFilterHiddenOnlyFallback(t *testing.T) {
	idxColType := plan.Type{Id: 27}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: catalog.PrefixIndexTableName + "unique_hash",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: idxColType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.NotEqual(t, catalog.IndexTableIndexColName, op.PkName,
		"hidden-only direct fuzzy filter must not leak internal index column name")
	require.NotContains(t, op.PkName, "__mo_",
		"hidden-only direct fuzzy filter must not surface any __mo_ internal name")
}

// TestConstructFuzzyFilterCompositeParentUniqueCols verifies the composite
// unique-index path surfaces a "(col1,col2)" tuple as PkName so runtime
// duplicate errors do not leak __mo_index_idx_col.
func TestConstructFuzzyFilterCompositeParentUniqueCols(t *testing.T) {
	idxColType := plan.Type{Id: 27}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: idxColType},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "t1",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "col1", Typ: idxColType},
				{Name: "col2", Typ: idxColType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "(col1,col2)", op.PkName)
}

func TestVectorToStringNullHandling(t *testing.T) {
	mp, err := mpool.NewMPool("test_vectorToString", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("per_row_null_check", func(t *testing.T) {
		// Vector with mix of NULLs and values
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(42), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(99), false, mp))
		defer vec.Free(mp)

		// Non-NULL row should return value
		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "42", s)

		// NULL row should return empty string
		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)

		// Non-NULL row after NULL should still return value
		s, err = vectorToString(vec, 2)
		require.NoError(t, err)
		require.Equal(t, "99", s)
	})

	t.Run("all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)

		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "", s)

		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)
	})

	t.Run("varchar_values", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp)) // NULL
		defer vec.Free(mp)

		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "hello", s)

		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)
	})

	// Cover more type branches in vectorToString
	t.Run("bool", func(t *testing.T) {
		vec := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(vec, true, false, mp))
		require.NoError(t, vector.AppendFixed(vec, false, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, false, false, mp))
		defer vec.Free(mp)

		s, _ := vectorToString(vec, 0)
		require.Equal(t, "true", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s) // NULL
		s, _ = vectorToString(vec, 2)
		require.Equal(t, "false", s)
	})

	t.Run("int_types", func(t *testing.T) {
		// int8
		vec8 := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(vec8, int8(7), false, mp))
		require.NoError(t, vector.AppendFixed(vec8, int8(0), true, mp))
		defer vec8.Free(mp)
		s, _ := vectorToString(vec8, 0)
		require.Equal(t, "7", s)
		s, _ = vectorToString(vec8, 1)
		require.Equal(t, "", s)

		// int16
		vec16 := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixed(vec16, int16(16), false, mp))
		require.NoError(t, vector.AppendFixed(vec16, int16(0), true, mp))
		defer vec16.Free(mp)
		s, _ = vectorToString(vec16, 0)
		require.Equal(t, "16", s)
		s, _ = vectorToString(vec16, 1)
		require.Equal(t, "", s)

		// int32
		vec32 := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, int32(32), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, int32(0), true, mp))
		defer vec32.Free(mp)
		s, _ = vectorToString(vec32, 0)
		require.Equal(t, "32", s)
		s, _ = vectorToString(vec32, 1)
		require.Equal(t, "", s)
	})

	t.Run("uint_types", func(t *testing.T) {
		vec8 := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(vec8, uint8(8), false, mp))
		require.NoError(t, vector.AppendFixed(vec8, uint8(0), true, mp))
		defer vec8.Free(mp)
		s, _ := vectorToString(vec8, 0)
		require.Equal(t, "8", s)
		s, _ = vectorToString(vec8, 1)
		require.Equal(t, "", s)

		vec16 := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixed(vec16, uint16(16), false, mp))
		require.NoError(t, vector.AppendFixed(vec16, uint16(0), true, mp))
		defer vec16.Free(mp)
		s, _ = vectorToString(vec16, 0)
		require.Equal(t, "16", s)

		vec32 := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, uint32(32), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, uint32(0), true, mp))
		defer vec32.Free(mp)
		s, _ = vectorToString(vec32, 0)
		require.Equal(t, "32", s)

		vec64 := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(vec64, uint64(64), false, mp))
		require.NoError(t, vector.AppendFixed(vec64, uint64(0), true, mp))
		defer vec64.Free(mp)
		s, _ = vectorToString(vec64, 0)
		require.Equal(t, "64", s)
	})

	t.Run("float_types", func(t *testing.T) {
		vec32 := vector.NewVec(types.T_float32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, float32(1.5), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, float32(0), true, mp))
		defer vec32.Free(mp)
		s, _ := vectorToString(vec32, 0)
		require.Equal(t, "1.5", s)
		s, _ = vectorToString(vec32, 1)
		require.Equal(t, "", s)

		vec64 := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(vec64, float64(2.5), false, mp))
		require.NoError(t, vector.AppendFixed(vec64, float64(0), true, mp))
		defer vec64.Free(mp)
		s, _ = vectorToString(vec64, 0)
		require.Equal(t, "2.5", s)
		s, _ = vectorToString(vec64, 1)
		require.Equal(t, "", s)
	})

	t.Run("decimal_types", func(t *testing.T) {
		dec64 := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		d64, _ := types.Decimal64FromFloat64(3.14, 64, 2)
		require.NoError(t, vector.AppendFixed(dec64, d64, false, mp))
		require.NoError(t, vector.AppendFixed(dec64, d64, true, mp)) // NULL
		defer dec64.Free(mp)
		s, _ := vectorToString(dec64, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(dec64, 1)
		require.Equal(t, "", s)

		dec128 := vector.NewVec(types.New(types.T_decimal128, 20, 2))
		d128, _ := types.Decimal128FromFloat64(6.28, 128, 2)
		require.NoError(t, vector.AppendFixed(dec128, d128, false, mp))
		require.NoError(t, vector.AppendFixed(dec128, d128, true, mp))
		defer dec128.Free(mp)
		s, _ = vectorToString(dec128, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(dec128, 1)
		require.Equal(t, "", s)
	})

	t.Run("bit_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_bit.ToType())
		require.NoError(t, vector.AppendFixed(vec, uint64(0xFF), false, mp))
		require.NoError(t, vector.AppendFixed(vec, uint64(0), true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Equal(t, "255", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("char_text_blob", func(t *testing.T) {
		for _, oid := range []types.T{types.T_char, types.T_text, types.T_blob, types.T_binary, types.T_varbinary} {
			vec := vector.NewVec(oid.ToType())
			require.NoError(t, vector.AppendBytes(vec, []byte("data"), false, mp))
			require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			defer vec.Free(mp)
			s, _ := vectorToString(vec, 0)
			require.Equal(t, "data", s, "type %v", oid)
			s, _ = vectorToString(vec, 1)
			require.Equal(t, "", s, "NULL for type %v", oid)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		vec := vector.NewVec(types.T_uuid.ToType())
		u, _ := types.ParseUuid("12345678-1234-1234-1234-123456789abc")
		require.NoError(t, vector.AppendFixed(vec, u, false, mp))
		require.NoError(t, vector.AppendFixed(vec, u, true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("date_time_types", func(t *testing.T) {
		// date
		dv := vector.NewVec(types.T_date.ToType())
		d, _ := types.ParseDateCast("2024-01-15")
		require.NoError(t, vector.AppendFixed(dv, d, false, mp))
		require.NoError(t, vector.AppendFixed(dv, d, true, mp))
		defer dv.Free(mp)
		s, _ := vectorToString(dv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(dv, 1)
		require.Equal(t, "", s)

		// time
		tv := vector.NewVec(types.T_time.ToType())
		tm, _ := types.ParseTime("12:30:45", 0)
		require.NoError(t, vector.AppendFixed(tv, tm, false, mp))
		require.NoError(t, vector.AppendFixed(tv, tm, true, mp))
		defer tv.Free(mp)
		s, _ = vectorToString(tv, 0)
		require.Contains(t, s, "12")
		s, _ = vectorToString(tv, 1)
		require.Equal(t, "", s)

		// datetime
		dtv := vector.NewVec(types.New(types.T_datetime, 0, 0))
		dt, _ := types.ParseDatetime("2024-01-15 12:30:45", 0)
		require.NoError(t, vector.AppendFixed(dtv, dt, false, mp))
		require.NoError(t, vector.AppendFixed(dtv, dt, true, mp))
		defer dtv.Free(mp)
		s, _ = vectorToString(dtv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(dtv, 1)
		require.Equal(t, "", s)

		// timestamp
		tsv := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		ts, _ := types.ParseTimestamp(time.Local, "2024-01-15 12:30:45", 0)
		require.NoError(t, vector.AppendFixed(tsv, ts, false, mp))
		require.NoError(t, vector.AppendFixed(tsv, ts, true, mp))
		defer tsv.Free(mp)
		s, _ = vectorToString(tsv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(tsv, 1)
		require.Equal(t, "", s)

		// year
		yv := vector.NewVec(types.T_year.ToType())
		require.NoError(t, vector.AppendFixed(yv, types.MoYear(2024), false, mp))
		require.NoError(t, vector.AppendFixed(yv, types.MoYear(0), true, mp))
		defer yv.Free(mp)
		s, _ = vectorToString(yv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(yv, 1)
		require.Equal(t, "", s)
	})

	t.Run("enum_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_enum.ToType())
		require.NoError(t, vector.AppendFixed(vec, types.Enum(3), false, mp))
		require.NoError(t, vector.AppendFixed(vec, types.Enum(0), true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Equal(t, "3", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("json_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_json.ToType())
		bj, _ := types.ParseStringToByteJson(`{"key":"val"}`)
		bjBytes, _ := bj.Marshal()
		require.NoError(t, vector.AppendBytes(vec, bjBytes, false, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Contains(t, s, "key")
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})
}

func TestFirstlyCheckSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test_firstlyCheck", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("all_nulls_no_error", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "all-NULL rows must not trigger duplicate error")
	})

	t.Run("nulls_mixed_with_distinct_values", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "NULLs should be skipped, 1 and 2 are distinct")
	})

	t.Run("real_dup_still_caught", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp)) // dup!
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.Error(t, err, "real duplicate must be caught")
		require.Contains(t, err.Error(), "Duplicate entry")
	})

	t.Run("compound_all_nulls_no_error", func(t *testing.T) {
		// Compound key: all rows are NULL (serial() propagated)
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "compound all-NULL rows must not trigger duplicate error")
	})

	t.Run("compound_mixed_nulls_distinct", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10) — non-NULL
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (2, 20) — non-NULL
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "NULLs should be skipped, (1,10) and (2,20) are distinct")
	})

	t.Run("compound_dup_caught", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10)
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (1, 10) — duplicate!
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.Error(t, err, "compound duplicate must be caught")
		require.Contains(t, err.Error(), "Duplicate entry")
	})
}

func TestGenCollsionKeysSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test_genCollsionKeys", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("non_compound_filters_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(3), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		require.Len(t, keys[0], 2, "should only have 2 non-NULL keys")
	})

	t.Run("non_compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		require.Len(t, keys[0], 0, "all NULLs should produce empty keys")
	})

	t.Run("compound_filters_nulls", func(t *testing.T) {
		// For compound keys, the packed tuple is a Varlena vector.
		// If ANY component column is NULL, serial() marks the tuple NULL.
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: non-NULL packed tuple
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL (component column was NULL)
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: non-NULL packed tuple
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		// 2 columns, each should have 2 non-NULL entries
		require.Len(t, keys, 2)
		require.Len(t, keys[0], 2, "column a should have 2 non-NULL keys")
		require.Len(t, keys[1], 2, "column b should have 2 non-NULL keys")
	})
}

func TestFillAllNullsSkipsBackgroundCheck(t *testing.T) {
	mp, err := mpool.NewMPool("test_fill", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("non_compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "pk"}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 0, f.cnt, "cnt should be 0 when all keys are NULL")
		require.Equal(t, "", f.condition, "condition should be empty")
	})

	t.Run("non_compound_mixed", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(42), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(99), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "pk"}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt, "cnt should only count non-NULL keys")
		require.NotEmpty(t, f.condition, "condition should be generated for non-NULL keys")
		// condition should contain the two non-NULL values
		require.Contains(t, f.condition, "42")
		require.Contains(t, f.condition, "99")
	})

	t.Run("compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 0, f.cnt, "compound all-NULLs should have cnt=0")
		require.Equal(t, "", f.condition)
	})

	t.Run("compound_mixed_nulls", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10) — non-NULL
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (2, 20) — non-NULL
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt, "compound mixed should count only 2 non-NULL keys")
		require.NotEmpty(t, f.condition)
	})
}

// TestFillOnlyInsertHidden tests the hidden unique index path (onlyInsertHidden=true).
// This covers the backfill/CREATE UNIQUE INDEX path where the hidden table receives
// only the unique column values.
func TestFillOnlyInsertHidden(t *testing.T) {
	mp, err := mpool.NewMPool("test_fill_hidden", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("hidden_with_values", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(10), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(20), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:             "pk",
			onlyInsertHidden: true,
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt)
		require.Contains(t, f.condition, "10")
		require.Contains(t, f.condition, "20")
	})

	t.Run("hidden_dup_caught_by_firstly_check_skipped", func(t *testing.T) {
		// onlyInsertHidden skips firstlyCheck — duplicates in the hidden table
		// are caught by backgroundSQLCheck, not the in-batch check.
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp)) // dup value

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:             "pk",
			onlyInsertHidden: true,
		}
		// fill should NOT error — firstlyCheck is skipped for hidden tables
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt)
	})
}

// TestFillEndToEndConditionGeneration exercises the full fill() pipeline:
// fill() → firstlyCheck → genCollsionKeys → condition string generation
// and verifies the generated SQL condition is valid for backgroundSQLCheck.
func TestFillEndToEndConditionGeneration(t *testing.T) {
	mp, err := mpool.NewMPool("test_e2e_fill", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("non_compound_nulls_skipped_real_dup_caught", func(t *testing.T) {
		// Multiple NULLs + a real duplicate value. firstlyCheck should catch the dup.
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(7), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(7), false, mp)) // dup!

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(4)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "pk"}
		err := f.fill(context.Background(), bat)
		require.Error(t, err, "real duplicate should be caught")
		require.Contains(t, err.Error(), "Duplicate entry")
	})

	t.Run("non_compound_nulls_only_skips_background_sql", func(t *testing.T) {
		// All NULLs → cnt=0, condition="" → backgroundSQLCheck would be skipped
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "name"}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 0, f.cnt, "all NULLs → cnt=0 → backgroundSQLCheck skipped")
		require.Equal(t, "", f.condition)
	})

	t.Run("compound_mixed_generates_valid_condition", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (10, 100) — non-NULL
		packer.Reset()
		packer.EncodeInt64(10)
		packer.EncodeInt64(100)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (20, 200) — non-NULL
		packer.Reset()
		packer.EncodeInt64(20)
		packer.EncodeInt64(200)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt)
		// Condition should contain both column names
		require.Contains(t, f.condition, "a =")
		require.Contains(t, f.condition, "b =")
	})
}

// TestConcurrentFirstlyCheck verifies that firstlyCheck is safe to call
// concurrently with independent fuzzyCheck instances (simulating concurrent INSERTs).
func TestConcurrentFirstlyCheck(t *testing.T) {
	mp, err := mpool.NewMPool("test_concurrent_fc", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	const numGoroutines = 8
	const numRows = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			// Each goroutine creates its own vector and fuzzyCheck
			vec := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < numRows; i++ {
				isNull := (i % 3) == 0 // every 3rd row is NULL
				if isNull {
					vector.AppendFixed(vec, int64(0), true, mp)
				} else {
					// Use goroutineID*numRows+i to ensure unique values across goroutines
					vector.AppendFixed(vec, int64(goroutineID*numRows+i), false, mp)
				}
			}
			defer vec.Free(mp)

			f := &fuzzyCheck{attr: "pk"}
			if err := f.firstlyCheck(context.Background(), vec); err != nil {
				errors <- err
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}
}
