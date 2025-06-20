package cdc

import (
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
	}
}

func TestNewFulltextSqlWriter(t *testing.T) {
	tabledef := newTestFulltextTableDef("id", types.T_int64, "body", types.T_varchar, 256)

	_, err := NewIndexSqlWriter("fulltext", tabledef, tabledef.Indexes)
	require.Nil(t, err)

}
