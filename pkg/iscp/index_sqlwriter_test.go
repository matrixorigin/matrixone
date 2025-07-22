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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func newTestIvfflatTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
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

func TestNewFulltextSqlWriter(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef("id", types.T_int64, "body", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()

	writer, err := NewIndexSqlWriter("fulltext", consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{int64(1000), []uint8("hello world"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{int64(2000), []uint8("hello world"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	fmt.Println(string(bytes))

}

func TestNewFulltextSqlWriterCPkey(t *testing.T) {
	var ctx context.Context

	tabledef := newTestFulltextTableDef2Parts("__mo_cpkey", types.T_varbinary, "body", "title", types.T_varchar, 256)
	consumerInfo := newTestConsumerInfo()

	writer, err := NewIndexSqlWriter("fulltext", consumerInfo, tabledef, tabledef.Indexes)
	require.Nil(t, err)

	row := []any{[]uint8("abcdef12"), []uint8("hello world"), []uint8("one title"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	row = []any{[]uint8("abc"), []uint8("hello world"), []uint8("two title"), nil}
	err = writer.Upsert(ctx, row)
	require.Nil(t, err)

	bytes, err := writer.ToSql()
	require.Nil(t, err)
	fmt.Println(string(bytes))

}

func TestNewHnswSqlWriter(t *testing.T) {
	var ctx context.Context

	tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 3)
	consumerInfo := newTestConsumerInfo()

	writer, err := NewHnswSqlWriter("fulltext", consumerInfo, tabledef, tabledef.Indexes)
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
	fmt.Println(string(bytes))
}

func TestNewIvfflatSqlWriter(t *testing.T) {
	var ctx context.Context

	tabledef := newTestIvfflatTableDef("pk", types.T_int64, "vec", types.T_array_float64, 3)
	consumerInfo := newTestConsumerInfo()

	writer, err := NewIvfflatSqlWriter("ivfflat", consumerInfo, tabledef, tabledef.Indexes)
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
	fmt.Println(string(bytes))
}
