package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func generatedColumnRefExpr(typ planpb.Type, pos int32, name string) *planpb.Expr {
	return &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: pos,
				Name:   name,
			},
		},
	}
}

func generatedDependencyTestTable() *planpb.TableDef {
	typ := planpb.Type{Id: int32(types.T_int64)}
	cols := []*planpb.ColDef{
		{Name: "source", Typ: typ},
		{Name: "middle", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 0, "source")}},
		{Name: "tail", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 1, "middle")}},
		{Name: "late_generated", Typ: typ, GeneratedCol: &planpb.GeneratedCol{Expr: generatedColumnRefExpr(typ, 4, "late_source")}},
		{Name: "late_source", Typ: typ},
		{Name: "other", Typ: typ},
	}
	name2ColIndex := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2ColIndex[col.Name] = int32(i)
	}
	return &planpb.TableDef{Cols: cols, Name2ColIndex: name2ColIndex}
}

func configureMockGeneratedPrimaryKey(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	base := mock.ctxt.tables["t_on_update_gen"]
	require.NotNil(t, base)
	base.Name2ColIndex = make(map[string]int32, len(base.Cols))
	for i, col := range base.Cols {
		base.Name2ColIndex[col.Name] = int32(i)
	}
	sourcePos, found := base.Name2ColIndex["val"]
	require.True(t, found)
	idPos, found := base.Name2ColIndex["id"]
	require.True(t, found)
	updatedPos, found := base.Name2ColIndex["updated_at"]
	require.True(t, found)
	idCol := base.Cols[idPos]
	idCol.GeneratedCol = &planpb.GeneratedCol{Expr: generatedColumnRefExpr(idCol.Typ, sourcePos, "val"), IsStored: true, OriginString: "val"}
	base.Cols[updatedPos].OnUpdate = nil
	base.Indexes = nil
}
