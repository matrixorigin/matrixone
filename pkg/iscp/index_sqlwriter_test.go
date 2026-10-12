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

package iscp

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type ivfflatIncludeColSpec struct {
	name  string
	typ   types.T
	width int32
	scale int32
}

func newTestIvfflatTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32, includeCols ...ivfflatIncludeColSpec) *plan.TableDef {
	name2ColIndex := map[string]int32{
		pkName:     0,
		vecColName: 1,
		"dummy":    2, // Add another col to make sure pk/vec col indices are used
	}
	cols := make([]*plan.ColDef, 0, 3+len(includeCols))
	cols = append(cols,
		&plan.ColDef{Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
		&plan.ColDef{Name: vecColName, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
		&plan.ColDef{Name: "dummy", Typ: plan.Type{Id: int32(types.T_int32)}},
	)
	includedColumns := make([]string, 0, len(includeCols))
	for _, includeCol := range includeCols {
		name2ColIndex[includeCol.name] = int32(len(cols))
		cols = append(cols, &plan.ColDef{Name: includeCol.name, Typ: plan.Type{Id: int32(includeCol.typ), Width: includeCol.width, Scale: includeCol.scale}})
		includedColumns = append(includedColumns, includeCol.name)
	}

	tableDef := &plan.TableDef{
		Name:          "test_orig_tbl",
		Name2ColIndex: name2ColIndex,
		Cols:          cols,
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "ivfidx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexTableName:     "meta_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "ivfidx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexTableName:     "centroids_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "ivfidx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexTableName:     "entries_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
		},
		DbName: "mydb",
	}
	for _, indexDef := range tableDef.Indexes {
		indexDef.IncludedColumns = append([]string(nil), includedColumns...)
	}
	return tableDef
}

func newTestFulltextTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name: "test_orig_tbl",
		Name2ColIndex: map[string]int32{
			pkName:     0,
			vecColName: 1,
			"dummy":    2, // Add another col to make sure pk/vec col indices are used
		},
		Cols: []*plan.ColDef{
			{Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
			{Name: vecColName, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
			{Name: "dummy", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "fulltext_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MOIndexFullTextAlgo.ToString(),
				IndexAlgoTableType: "",
				IndexTableName:     "fulltext_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"parser":"ngram"}`,
			},
		},
		DbName: "mydb",
	}
}

func newTestFulltextTableDef2Parts(pkName string, pkType types.T, vecColName string, vecColName2 string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name: "test_orig_tbl",
		Name2ColIndex: map[string]int32{
			pkName:      0,
			vecColName:  1,
			vecColName2: 2,
			"dummy":     3, // Add another col to make sure pk/vec col indices are used
		},
		Cols: []*plan.ColDef{
			{Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
			{Name: vecColName, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
			{Name: vecColName2, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
			{Name: "dummy", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "fulltext_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MOIndexFullTextAlgo.ToString(),
				IndexAlgoTableType: "",
				IndexTableName:     "fulltext_tbl",
				Parts:              []string{vecColName, vecColName2},
				IndexAlgoParams:    `{"parser":"ngram"}`,
			},
		},
		DbName: "mydb",
	}
}

func TestNewFulltextSqlWriterUpsert(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef("id", types.T_int64, "body", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewFulltextSqlWriter("fulltext", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{int64(1000), []uint8("hello world"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), nil, nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "REPLACE INTO `mydb`.`fulltext_tbl` WITH src as (SELECT CAST(column_0 as BIGINT) as `id`, CAST(column_1 as VARCHAR(256)) as `body` FROM (VALUES ROW(1000,'hello world'),ROW(2000,CAST(NULL as VARCHAR(256)))) as __mo_iscp_values) SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('', 23, id, body) as f", string(bytes))
}

func TestNewFulltextSqlWriterInsert(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef("id", types.T_int64, "body", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewFulltextSqlWriter("fulltext", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{int64(1000), []uint8("hello world"), nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), nil, nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "REPLACE INTO `mydb`.`fulltext_tbl` WITH src as (SELECT CAST(column_0 as BIGINT) as `id`, CAST(column_1 as VARCHAR(256)) as `body` FROM (VALUES ROW(1000,'hello world'),ROW(2000,CAST(NULL as VARCHAR(256)))) as __mo_iscp_values) SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('', 23, id, body) as f", string(bytes))
}

func TestNewFulltextSqlWriterDelete(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef("id", types.T_int64, "body", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewFulltextSqlWriter("fulltext", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{int64(1000), []uint8("hello world"), nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), nil, nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "DELETE FROM `test_db`.`fulltext_tbl` WHERE `doc_id` IN (1000,2000)", string(bytes))
}

func TestNewFulltextSqlWriterCPkey(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef2Parts("__mo_cpkey", types.T_varbinary, "body", "title", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewFulltextSqlWriter("fulltext", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{[]uint8("abcdef12"), []uint8("hello world"), []uint8("one title"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{[]uint8("abc"), []uint8("hello world"), []uint8("two title"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "REPLACE INTO `mydb`.`fulltext_tbl` WITH src as (SELECT CAST(column_0 as VARBINARY(0)) as `__mo_cpkey`, CAST(column_1 as VARCHAR(256)) as `body`, CAST(column_2 as VARCHAR(256)) as `title` FROM (VALUES ROW(x'6162636465663132','hello world','one title'),ROW(x'616263','hello world','two title')) as __mo_iscp_values) SELECT f.* FROM src CROSS APPLY fulltext_index_tokenize('', 65, __mo_cpkey, body, title) as f", string(bytes))
}

func TestNewHnswSqlWriter(t *testing.T) {
	var ctx context.Context

	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 3)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewHnswSqlWriter("fulltext", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)
	row := []any{int64(1000), []float32{1, 2, 3}, nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), []float32{5, 6, 7}, nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(3000), []float32{5, 6, 7}, nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.JSONEq(t, `{"cdc":[{"t":"U","pk":1000,"v":[1,2,3]},{"t":"I","pk":2000,"v":[5,6,7]},{"t":"D","pk":3000}]}`, string(bytes))
}

func TestNewIvfflatSqlWriterInsert(t *testing.T) {
	var ctx context.Context

	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float64, 3)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewIvfflatSqlWriter("ivfflat", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)
	row := []any{int64(1000), []float64{1, 2, 3}, nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), []float64{5, 6, 7}, nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(3000), []float64{5, 6, 7}, nil}
	err = writer.Insert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "REPLACE INTO `test_db`.`entries_tbl` (`__mo_index_centroid_fk_version`, `__mo_index_centroid_fk_id`, `__mo_index_pri_col`, `__mo_index_centroid_fk_entry`) WITH centroid as (SELECT * FROM `test_db`.`centroids_tbl` WHERE `__mo_index_centroid_version` = (SELECT CAST(__mo_index_val as BIGINT) FROM `test_db`.`meta_tbl` WHERE `__mo_index_key` = 'version') ), src as (SELECT CAST(column_0 as BIGINT) as `src0`, CAST(column_1 as VECF64(3)) as `src1` FROM (VALUES ROW(1000,CAST('[1, 2, 3]' as VECF64(3))),ROW(2000,CAST('[5, 6, 7]' as VECF64(3))),ROW(3000,CAST('[5, 6, 7]' as VECF64(3)))) as __mo_iscp_values) SELECT `__mo_index_centroid_version`, `__mo_index_centroid_id`, src0, src1 FROM src CENTROIDX('vector_l2_ops') JOIN centroid using (`__mo_index_centroid`, `src1`)", string(bytes))
}

func TestNewIvfflatSqlWriterUpsert(t *testing.T) {
	var ctx context.Context

	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float64, 3)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewIvfflatSqlWriter("ivfflat", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)
	row := []any{int64(1000), []float64{1, 2, 3}, nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), []float64{5, 6, 7}, nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(3000), []float64{5, 6, 7}, nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "REPLACE INTO `test_db`.`entries_tbl` (`__mo_index_centroid_fk_version`, `__mo_index_centroid_fk_id`, `__mo_index_pri_col`, `__mo_index_centroid_fk_entry`) WITH centroid as (SELECT * FROM `test_db`.`centroids_tbl` WHERE `__mo_index_centroid_version` = (SELECT CAST(__mo_index_val as BIGINT) FROM `test_db`.`meta_tbl` WHERE `__mo_index_key` = 'version') ), src as (SELECT CAST(column_0 as BIGINT) as `src0`, CAST(column_1 as VECF64(3)) as `src1` FROM (VALUES ROW(1000,CAST('[1, 2, 3]' as VECF64(3))),ROW(2000,CAST('[5, 6, 7]' as VECF64(3))),ROW(3000,CAST('[5, 6, 7]' as VECF64(3)))) as __mo_iscp_values) SELECT `__mo_index_centroid_version`, `__mo_index_centroid_id`, src0, src1 FROM src CENTROIDX('vector_l2_ops') JOIN centroid using (`__mo_index_centroid`, `src1`)", string(bytes))
}

func TestNewIvfflatSqlWriterUpsertWithIncludeColumns(t *testing.T) {
	var ctx context.Context

	tabledef := newTestIvfflatTableDef(
		"pk",
		types.T_int64,
		"vec",
		types.T_array_float64,
		3,
		ivfflatIncludeColSpec{name: "title", typ: types.T_varchar, width: 64},
		ivfflatIncludeColSpec{name: "rank", typ: types.T_int32},
	)
	// Catalog-loaded definitions do not carry the runtime-only proto field. IVF
	// INCLUDE metadata must therefore remain recoverable from algo_params.
	for _, indexDef := range tabledef.Indexes {
		indexDef.IncludedColumns = nil
		indexDef.IndexAlgoParams = `{"included_columns":"title,rank","lists":"16","op_type":"vector_l2_ops"}`
	}
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewIvfflatSqlWriter("ivfflat", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.NoError(t, err)

	err = writer.Upsert(ctx, []any{int64(1000), []float64{1, 2, 3}, nil, []byte("news"), int32(7)})
	require.NoError(t, err)
	err = writer.Upsert(ctx, []any{int64(2000), []float64{5, 6, 7}, nil, nil, int32(9)})
	require.NoError(t, err)

	bytes, err := writer.ToSql()
	require.NoError(t, err)
	sql := string(bytes)
	require.Contains(t, sql, "`__mo_index_include_title`, `__mo_index_include_rank`")
	require.Contains(t, sql, "CAST(column_2 as VARCHAR")
	require.Contains(t, sql, "CAST(column_3 as INT")
	require.Contains(t, sql, "src0, src1, src2, src3")
	require.Contains(t, sql, "'news'")
	require.Contains(t, sql, "CAST(NULL as")
}

func TestNewIvfflatSqlWriterAsyncReloadPreservesIncludeColumns(t *testing.T) {
	includeColumns := []string{"title, label", " rank "}
	tabledef := newTestIvfflatTableDef(
		"pk",
		types.T_int64,
		"vec",
		types.T_array_float64,
		3,
		ivfflatIncludeColSpec{name: includeColumns[0], typ: types.T_varchar, width: 64},
		ivfflatIncludeColSpec{name: includeColumns[1], typ: types.T_int32},
	)
	encoded, err := catalog.MarshalIncludeColumnsValue(includeColumns)
	require.NoError(t, err)
	algoParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IncludedColumns:      encoded,
		catalog.IndexAlgoParamLists:  "16",
		catalog.IndexAlgoParamOpType: "vector_l2_ops",
	})
	require.NoError(t, err)
	for _, indexDef := range tabledef.Indexes {
		// ISCP reconstructs its async DML writer from catalog metadata, where the
		// runtime-only proto field is absent.
		indexDef.IncludedColumns = nil
		indexDef.IndexAlgoParams = algoParams
	}

	writer, err := NewIvfflatSqlWriter(
		"ivfflat", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
	require.NoError(t, err)
	require.Equal(t, includeColumns, writer.(*IvfflatSqlWriter).includeCols)

	err = writer.Upsert(context.Background(), []any{
		int64(1000), []float64{1, 2, 3}, nil, []byte("news"), int32(7),
	})
	require.NoError(t, err)
	bytes, err := writer.ToSql()
	require.NoError(t, err)
	sql := string(bytes)
	require.Contains(t, sql, "`__mo_index_include_title, label`, `__mo_index_include_ rank `")
	require.Contains(t, sql, "'news'")
}

func TestNewIvfflatSqlWriterPreservesTemporalIncludeScale(t *testing.T) {
	for _, scale := range []int32{0, 3, 6} {
		t.Run(fmt.Sprintf("scale%d", scale), func(t *testing.T) {
			tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2,
				ivfflatIncludeColSpec{name: "ts", typ: types.T_timestamp, scale: scale},
				ivfflatIncludeColSpec{name: "dt", typ: types.T_datetime, scale: scale},
				ivfflatIncludeColSpec{name: "tm", typ: types.T_time, scale: scale})
			writer, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
			require.NoError(t, err)
			for _, write := range []func(context.Context, []any) error{writer.Insert, writer.Upsert} {
				require.NoError(t, write(t.Context(), []any{int64(1), []float32{1, 2}, nil,
					"2026-01-02 03:04:05.123456", "2026-01-02 03:04:05.123456", "03:04:05.123456"}))
				require.NoError(t, write(t.Context(), []any{int64(2), []float32{2, 1}, nil, nil, nil, nil}))
				sqlBytes, err := writer.ToSql()
				require.NoError(t, err)
				sql := string(sqlBytes)
				for i, typ := range []string{"TIMESTAMP", "DATETIME", "TIME"} {
					require.Contains(t, sql, fmt.Sprintf("CAST(column_%d as %s(%d))", i+2, typ, scale))
				}
				require.Contains(t, sql, fmt.Sprintf("AS TIMESTAMP(%d))", scale))
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				stmt.Free()
			}
		})
	}
}

func TestNewIvfflatSqlWriterAffineQuantizationUsesVectorBeforeInclude(t *testing.T) {
	for _, quantization := range []string{"int8", "uint8"} {
		for _, opType := range []string{"vector_l2_ops", "vector_l2sq_ops"} {
			t.Run(quantization+"/"+opType, func(t *testing.T) {
				tabledef := newTestIvfflatTableDef(
					"pk", types.T_enum, "vec", types.T_array_float32, 3,
					ivfflatIncludeColSpec{name: "rank", typ: types.T_int32},
					ivfflatIncludeColSpec{name: "status", typ: types.T_enum},
				)
				tabledef.Cols[0].Typ.Enumvalues = "first,second"
				tabledef.Cols[4].Typ.Enumvalues = "cold,hot"
				for _, indexDef := range tabledef.Indexes {
					indexDef.IndexAlgoParams = `{"included_columns":"rank,status","lists":"16","op_type":"` + opType + `","quantization":"` + quantization + `"}`
				}
				writer, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
				require.NoError(t, err)
				for _, write := range []func(context.Context, []any) error{writer.Insert, writer.Upsert} {
					require.NoError(t, write(t.Context(), []any{types.Enum(1), []float32{1, 2, 3}, nil, int32(7), types.Enum(2)}))
					require.NoError(t, write(t.Context(), []any{types.Enum(2), []float32{3, 2, 1}, nil, nil, nil}))
					sqlBytes, err := writer.ToSql()
					require.NoError(t, err)
					sql := string(sqlBytes)
					require.Contains(t, sql, "SERIAL_EXTRACT(src0, 0 as ENUM('first','second')), ")
					require.Contains(t, sql, "), src2, SERIAL_EXTRACT(src3, 0 as ENUM('cold','hot')) FROM src")
					require.Contains(t, sql, "cast(cast(src1")
					require.NotContains(t, sql, "src2 *")
					require.NotContains(t, sql, "src3 *")
					stmt, err := mysql.ParseOne(t.Context(), sql, 1)
					require.NoError(t, err)
					stmt.Free()
				}
			})
		}
	}
}

func TestNewIvfflatSqlWriterDelete(t *testing.T) {
	var ctx context.Context

	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float64, 3)
	consumerInfo := newTestConsumerInfo()
	jobID := newTestJobID()

	writer, err := NewIvfflatSqlWriter("ivfflat", jobID, consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)
	row := []any{int64(1000), []float64{1, 2, 3}, nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), []float64{5, 6, 7}, nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	row = []any{int64(3000), []float64{5, 6, 7}, nil}
	err = writer.Delete(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	require.Equal(t, "DELETE FROM `test_db`.`entries_tbl` WHERE `__mo_index_pri_col` IN (1000,2000,3000)", string(bytes))
}

func TestIvfflatSqlWriterEnumReplay(t *testing.T) {
	for _, tc := range []struct {
		name, pkMembers, includeMembers string
	}{
		{"distinct domains", "cold,hot,warm", "warm,cold,hot"},
		{"numeric and escaped labels", "2,1", "a'b,c\\d"},
		{"empty and numeric labels", ",2,1", ",2,1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tabledef := newTestIvfflatTableDef("pk", types.T_enum, "vec", types.T_array_float32, 2,
				ivfflatIncludeColSpec{name: "status", typ: types.T_enum})
			tabledef.Cols[0].Typ.Enumvalues = tc.pkMembers
			tabledef.Cols[3].Typ.Enumvalues = tc.includeMembers
			// Exercise the catalog reload path, without runtime INCLUDE metadata.
			for _, idx := range tabledef.Indexes {
				idx.IncludedColumns = nil
				idx.IndexAlgoParams = `{"included_columns":"status","lists":"1","op_type":"vector_l2_ops"}`
			}
			writer, err := NewIvfflatSqlWriter("ivfflat", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
			require.NoError(t, err)
			for _, upsert := range []bool{false, true} {
				writer.Reset()
				write := writer.Insert
				if upsert {
					write = writer.Upsert
				}
				require.NoError(t, write(t.Context(), []any{types.Enum(1), []float32{0, 0}, nil, types.Enum(2)}))
				require.NoError(t, write(t.Context(), []any{types.Enum(2), []float32{1, 0}, nil, nil}))
				require.NoError(t, write(t.Context(), []any{types.Enum(0), []float32{2, 0}, nil, types.Enum(0)}))
				generated, err := writer.ToSql()
				require.NoError(t, err)
				sql := string(generated)
				require.Contains(t, sql, "VALUES ROW(x'503d1501',CAST('[0, 0]' as VECF32(2)),x'503d1502'),ROW(x'503d1502',CAST('[1, 0]' as VECF32(2)),x'00'),ROW(x'503d14',CAST('[2, 0]' as VECF32(2)),x'503d14')")
				stmt, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				stmt.Free()
				// Inspect parsed domains from the actual generated tuple decoders.
				for _, col := range []struct {
					pattern, members string
				}{
					{`(SERIAL_EXTRACT\(src0, 0 as ENUM\(.*?\)\))`, tc.pkMembers},
					{`(SERIAL_EXTRACT\(src2, 0 as ENUM\(.*?\)\))`, tc.includeMembers},
				} {
					match := regexp.MustCompile(col.pattern).FindStringSubmatch(sql)
					require.Len(t, match, 2)
					parsed, err := mysql.ParseOne(t.Context(), "SELECT "+match[1], 1)
					require.NoError(t, err)
					decode := parsed.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.SerialExtractExpr)
					require.Equal(t, strings.Split(col.members, ","), decode.ResultType.(*tree.T).InternalType.EnumValues)
					parsed.Free()
				}
			}
			writer.Reset()
			require.NoError(t, writer.Delete(t.Context(), []any{types.Enum(1)}))
			require.NoError(t, writer.Delete(t.Context(), []any{types.Enum(0)}))
			deleted, err := writer.ToSql()
			require.NoError(t, err)
			require.Equal(t, "DELETE FROM `test_db`.`entries_tbl` WHERE `__mo_index_pri_col` IN (1,0)", string(deleted))
		})
	}
}
