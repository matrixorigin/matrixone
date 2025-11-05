// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCDCStatementBuilder(t *testing.T) {
	t.Run("ValidBuilder_SinglePK", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Name: "test_table",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey: &plan.PrimaryKeyDef{
				Names: []string{"id"},
			},
			Name2ColIndex: map[string]int32{"id": 0, "name": 1},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test_table", tableDef, 1024*1024, false)

		require.NoError(t, err)
		require.NotNil(t, builder)
		assert.Equal(t, "test_db", builder.dbName)
		assert.Equal(t, "test_table", builder.tableName)
		assert.Equal(t, 2, len(builder.insertColTypes))
		assert.Equal(t, 1, len(builder.pkColNames))
		assert.Equal(t, "id", builder.pkColNames[0])
		assert.True(t, builder.isSinglePK)
		assert.False(t, builder.isMO)
	})

	t.Run("ValidBuilder_CompositePK", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Name: "test_table",
			Cols: []*plan.ColDef{
				{Name: "id1", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "id2", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey: &plan.PrimaryKeyDef{
				Names: []string{"id1", "id2"},
			},
			Name2ColIndex: map[string]int32{"id1": 0, "id2": 1, "name": 2},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test_table", tableDef, 1024*1024, true)

		require.NoError(t, err)
		require.NotNil(t, builder)
		assert.Equal(t, 3, len(builder.insertColTypes))
		assert.Equal(t, 2, len(builder.pkColNames))
		assert.Equal(t, []string{"id1", "id2"}, builder.pkColNames)
		assert.False(t, builder.isSinglePK)
		assert.True(t, builder.isMO)
	})

	t.Run("ExcludesInternalColumns", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Name: "test_table",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}}, // Internal column
			},
			Pkey: &plan.PrimaryKeyDef{
				Names: []string{"id"},
			},
			Name2ColIndex: map[string]int32{"id": 0, "name": 1, catalog.Row_ID: 2},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test_table", tableDef, 1024*1024, false)

		require.NoError(t, err)
		// Should only have 2 columns (id, name), excluding __mo_rowid
		assert.Equal(t, 2, len(builder.insertColTypes))
	})

	t.Run("NilTableDef", func(t *testing.T) {
		_, err := NewCDCStatementBuilder("test_db", "test_table", nil, 1024*1024, false)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tableDef is required")
	})
}

func TestCDCStatementBuilder_BuildInsertSQL(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Name: "users",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "age", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "name": 1, "age": 2},
	}

	builder, err := NewCDCStatementBuilder("test_db", "users", tableDef, 1024*1024, false)
	require.NoError(t, err)

	t.Run("SimpleInsert_OneRow", func(t *testing.T) {
		// Create batch with 1 row
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Alice"), false, mp)
		vector.AppendFixed(bat.Vecs[2], int32(25), false, mp)
		bat.SetRowCount(1)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1, "Should generate exactly 1 SQL statement")

		sql := string(sqls[0][v2SQLBufReserved:])
		t.Logf("Generated SQL: %s", sql)

		// Verify SQL structure
		assert.Contains(t, sql, "/* [100-0, 200-0) */")
		assert.Contains(t, sql, "REPLACE INTO `test_db`.`users` VALUES")
		assert.Contains(t, sql, "(1,'Alice',25)")
		assert.True(t, strings.HasSuffix(sql, ";"))
	})

	t.Run("SimpleInsert_MultipleRows", func(t *testing.T) {
		// Create batch with 3 rows
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Alice"), false, mp)
		vector.AppendFixed(bat.Vecs[2], int32(25), false, mp)

		vector.AppendFixed(bat.Vecs[0], int32(2), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Bob"), false, mp)
		vector.AppendFixed(bat.Vecs[2], int32(30), false, mp)

		vector.AppendFixed(bat.Vecs[0], int32(3), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Charlie"), false, mp)
		vector.AppendFixed(bat.Vecs[2], int32(35), false, mp)

		bat.SetRowCount(3)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1)

		sql := string(sqls[0][v2SQLBufReserved:])
		t.Logf("Generated SQL: %s", sql)

		// Verify all rows are included
		assert.Contains(t, sql, "(1,'Alice',25)")
		assert.Contains(t, sql, "(2,'Bob',30)")
		assert.Contains(t, sql, "(3,'Charlie',35)")

		// Verify comma separation
		assert.Contains(t, sql, "),(")
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
		bat.SetRowCount(0)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		assert.Nil(t, sqls, "Should return nil for empty batch")
	})

	t.Run("NilBatch", func(t *testing.T) {
		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, nil, fromTs, toTs)

		require.NoError(t, err)
		assert.Nil(t, sqls)
	})

	t.Run("NullValues", func(t *testing.T) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], nil, true, mp)      // NULL value
		vector.AppendFixed(bat.Vecs[2], int32(0), true, mp) // NULL value
		bat.SetRowCount(1)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1)

		sql := string(sqls[0][v2SQLBufReserved:])
		t.Logf("Generated SQL: %s", sql)

		// Verify NULL handling
		assert.Contains(t, sql, "(1,NULL,NULL)")
	})

	t.Run("SpecialCharactersInString", func(t *testing.T) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("It's a test"), false, mp) // Single quote
		vector.AppendFixed(bat.Vecs[2], int32(25), false, mp)
		bat.SetRowCount(1)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1)

		sql := string(sqls[0][v2SQLBufReserved:])
		t.Logf("Generated SQL: %s", sql)

		// Verify special characters are escaped
		assert.Contains(t, sql, "It\\'s a test")
	})
}

func TestCDCStatementBuilder_BuildInsertSQL_SizeLimit(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Name: "large_table",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "data", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "data": 1},
	}

	// Small max size to force splitting
	builder, err := NewCDCStatementBuilder("test_db", "large_table", tableDef, 300, false)
	require.NoError(t, err)

	t.Run("SplitLargeBatch", func(t *testing.T) {
		// Create batch with multiple rows
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		// Add rows that will exceed 300 bytes when combined
		for i := 0; i < 10; i++ {
			vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
			vector.AppendBytes(bat.Vecs[1], []byte(strings.Repeat("X", 20)), false, mp)
		}
		bat.SetRowCount(10)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		assert.Greater(t, len(sqls), 1, "Should split into multiple SQL statements")

		// Verify all SQLs have proper structure
		for i, sql := range sqls {
			sqlStr := string(sql[v2SQLBufReserved:])
			t.Logf("SQL %d (len=%d): %s", i+1, len(sql), sqlStr)

			assert.Contains(t, sqlStr, "REPLACE INTO")
			assert.True(t, strings.HasSuffix(sqlStr, ";"))

			// Each SQL should be within size limit
			assert.LessOrEqual(t, len(sql), 300, "Each SQL should be within size limit")
		}

		// Verify we got multiple SQL statements (proving splitting works)
		assert.GreaterOrEqual(t, len(sqls), 2, "Should split into at least 2 SQL statements")
	})
}

func TestCDCStatementBuilder_BuildPKColumnList(t *testing.T) {
	t.Run("SinglePK", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Name: "test",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0},
		}

		builder, err := NewCDCStatementBuilder("db", "test", tableDef, 1024, false)
		require.NoError(t, err)

		pkList := builder.buildPKColumnList()
		assert.Equal(t, "id", pkList)
	})

	t.Run("CompositePK", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Name: "test",
			Cols: []*plan.ColDef{
				{Name: "id1", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "id2", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "id3", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id1", "id2", "id3"}},
			Name2ColIndex: map[string]int32{"id1": 0, "id2": 1, "id3": 2},
		}

		builder, err := NewCDCStatementBuilder("db", "test", tableDef, 1024, false)
		require.NoError(t, err)

		pkList := builder.buildPKColumnList()
		assert.Equal(t, "(id1,id2,id3)", pkList)
	})
}

func TestCDCStatementBuilder_EstimateRowSize(t *testing.T) {
	tableDef := &plan.TableDef{
		Name: "test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "age", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "name": 1, "age": 2},
	}

	builder, err := NewCDCStatementBuilder("db", "test", tableDef, 1024, false)
	require.NoError(t, err)

	t.Run("EstimateInsertSize", func(t *testing.T) {
		size := builder.EstimateInsertRowSize()
		// 3 columns * 50 bytes = 150 bytes
		assert.Equal(t, 150, size)
	})

	t.Run("EstimateDeleteSize_SinglePK", func(t *testing.T) {
		size := builder.EstimateDeleteRowSize()
		assert.Equal(t, 50, size)
	})
}

// Additional test cases based on old sinker tests

func TestCDCStatementBuilder_VariousDataTypes(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Name: "type_test",
		Cols: []*plan.ColDef{
			{Name: "col_uint64", Typ: plan.Type{Id: int32(types.T_uint64)}},
			{Name: "col_varchar", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "col_bool", Typ: plan.Type{Id: int32(types.T_bool)}},
			{Name: "col_float64", Typ: plan.Type{Id: int32(types.T_float64)}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"col_uint64"}},
		Name2ColIndex: map[string]int32{
			"col_uint64": 0, "col_varchar": 1, "col_bool": 2, "col_float64": 3,
		},
	}

	builder, err := NewCDCStatementBuilder("test_db", "type_test", tableDef, 1024*1024, false)
	require.NoError(t, err)

	t.Run("MultipleDataTypes", func(t *testing.T) {
		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_bool.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_float64.ToType())

		vector.AppendFixed(bat.Vecs[0], uint64(12345), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("test"), false, mp)
		vector.AppendFixed(bat.Vecs[2], true, false, mp)
		vector.AppendFixed(bat.Vecs[3], float64(3.14), false, mp)
		bat.SetRowCount(1)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1)

		sql := string(sqls[0][v2SQLBufReserved:])
		t.Logf("Generated SQL: %s", sql)

		assert.Contains(t, sql, "12345")  // uint64
		assert.Contains(t, sql, "'test'") // varchar
		assert.Contains(t, sql, "true")   // bool
		assert.Contains(t, sql, "3.14")   // float64
	})
}

func TestCDCStatementBuilder_StringEscaping(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Name: "test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "text", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "text": 1},
	}

	builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Backslash",
			input:    "path\\to\\file",
			expected: "path\\\\to\\\\file",
		},
		{
			name:     "SingleQuote",
			input:    "It's",
			expected: "It\\'s",
		},
		{
			name:     "BackslashAndQuote",
			input:    "path\\'s",
			expected: "path\\\\\\'s",
		},
		{
			name:     "EmptyString",
			input:    "",
			expected: "''",
		},
		{
			name:     "Unicode",
			input:    "测试_中文",
			expected: "测试_中文",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

			vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
			vector.AppendBytes(bat.Vecs[1], []byte(tc.input), false, mp)
			bat.SetRowCount(1)

			ctx := context.Background()
			fromTs := types.BuildTS(100, 0)
			toTs := types.BuildTS(200, 0)

			sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

			require.NoError(t, err)
			require.Len(t, sqls, 1)

			sql := string(sqls[0][v2SQLBufReserved:])
			t.Logf("Input: %q -> SQL: %s", tc.input, sql)

			assert.Contains(t, sql, tc.expected, "Should properly handle: %s", tc.name)
		})
	}
}

func TestCDCStatementBuilder_LargeValues(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableDef := &plan.TableDef{
		Name: "test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "data", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "data": 1},
	}

	builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	require.NoError(t, err)

	t.Run("VeryLongString", func(t *testing.T) {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		longStr := strings.Repeat("A", 10000)
		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte(longStr), false, mp)
		bat.SetRowCount(1)

		ctx := context.Background()
		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)

		sqls, err := builder.BuildInsertSQL(ctx, bat, fromTs, toTs)

		require.NoError(t, err)
		require.Len(t, sqls, 1)

		sql := string(sqls[0][v2SQLBufReserved:])
		assert.Contains(t, sql, longStr)
		assert.Greater(t, len(sql), 10000)
	})
}
