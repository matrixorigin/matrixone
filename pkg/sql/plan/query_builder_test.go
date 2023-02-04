package plan

import (
	"context"
	"encoding/json"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildTable_AlterView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}
	store := make(map[string]arg)

	store["db.a"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: &plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "a",
					},
				},
			},
		}}

	vData, err := json.Marshal(ViewData{
		"create view v as select a from a",
		"db",
	})
	assert.NoError(t, err)

	store["db.v"] = arg{nil,
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vData),
			}},
	}
	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
		}).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(&plan.Stats{}).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()

	qb := NewQueryBuilder(plan.Query_SELECT, ctx)
	tb := &tree.TableName{}
	tb.SchemaName = "db"
	tb.ObjectName = "v"
	bc := NewBindContext(qb, nil)
	_, err = qb.buildTable(tb, bc)
	assert.Error(t, err)
}
