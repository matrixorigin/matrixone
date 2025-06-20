package cdc

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

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
	dbTableInfo := newTestDbTableInfo()

	writer, err := NewIndexSqlWriter("fulltext", dbTableInfo, tabledef, tabledef.Indexes)
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
	dbTableInfo := newTestDbTableInfo()

	writer, err := NewIndexSqlWriter("fulltext", dbTableInfo, tabledef, tabledef.Indexes)
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
