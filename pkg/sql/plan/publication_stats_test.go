package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexJoinPublicationUsesCompleteFilters(t *testing.T) {
	for _, rows := range []float64{100000000, 0} {
		name := "known"
		if rows == 0 {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			ctx := &backfillTopStatsContext{MockCompilerContext: NewMockCompilerContext(true), rows: rows}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			typ := planpb.Type{Id: int32(types.T_int32)}
			table := &planpb.TableDef{Name: "t", TblId: 100001, Cols: []*planpb.ColDef{{Name: "id", Typ: typ}, {Name: "a", Typ: typ}, {Name: "b", Typ: typ}},
				Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2}, Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}
			eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(typ, 10, 1), makePlan2Int32ConstExprWithType(7)})
			require.NoError(t, err)
			bounds, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "between", []*planpb.Expr{GetColExpr(typ, 10, 2), makePlan2Int32ConstExprWithType(1), makePlan2Int32ConstExprWithType(3)})
			require.NoError(t, err)
			base := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: &planpb.ObjectRef{Obj: 100001, SchemaName: "db", ObjName: "t"}, BindingTags: []int32{10}, FilterList: []*planpb.Expr{eq, bounds}}
			builder.appendNode(base, nil)
			idx := &planpb.IndexDef{IndexName: "ix_ab", Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true, IndexTableName: "idx"}
			idxTable := makeTestIndexTableDef()
			idxTable.Name = "idx"
			idxTable.TblId = 100002
			idxTable.Cols[1].Typ = typ
			leading := builder.replaceEqualCondition(idx, base.FilterList, []int32{0}, 20, idxTable)
			prepared := &preparedIndexJoin{idxDef: idx, idxTag: 20, idxObjRef: &planpb.ObjectRef{Obj: 100002, SchemaName: "db", ObjName: "idx"}, idxTableDef: idxTable, idxFilter: leading}
			joinID, indexID := builder.publishIndexJoin(prepared, base, EqualIndexCondition, []int32{0})
			scan := builder.qry.Nodes[indexID]
			join := builder.qry.Nodes[joinID]
			require.Greater(t, len(scan.FilterList), 1)
			require.Equal(t, "prefix_between", scan.FilterList[0].GetF().Func.ObjName)
			if rows == 0 {
				require.True(t, IsDefaultStats(scan.Stats))
				return
			}
			// The fully filtered estimate must shrink the leading-only candidates,
			// and remain above the unrelated historical1000-row scheduling cap.
			require.Greater(t, scan.Stats.Outcnt, float64(1000))
			require.Less(t, scan.Stats.Outcnt, rows/1000)
			require.Equal(t, base.Stats.Cost+scan.Stats.Cost, join.Stats.Cost)
			require.Equal(t, indexID, join.Children[1])
		})
	}
}
