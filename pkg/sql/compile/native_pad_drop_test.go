// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNativePADDropRechecksResolvedCatalog(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	c.disableLock, c.ignorePublish = true, true
	restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion129)
	table := &planpb.TableDef{Name: "v", ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view v as select 'A' collate utf8mb4_general_ci='a' as b","collation_binding_revision":1,"required_protocol_version":129}`}, Cols: []*planpb.ColDef{{Name: "b", Typ: types.T_bool.ToType().PlanType()}}}
	ctrl := gomock.NewController(t)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	resolved, published := false, false
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(func(context.Context) *planpb.TableDef {
		resolved = true
		moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
		return table
	}).AnyTimes()
	db.EXPECT().Delete(gomock.Any(), "v").DoAndReturn(func(context.Context, string) error {
		published = true
		return moerr.NewInternalErrorNoCtx("unexpected deletion after writer floor fell")
	}).AnyTimes()
	admitted := true
	drop := &planpb.DropTable{Database: "db", Table: "v", IsView: true, TableDef: table}
	err := (&Scope{}).dropTableSingleResolved(c, drop, &admitted, true, db, rel, nil, nil, nil, false)
	require.ErrorContains(t, err, "protocol version 129")
	require.True(t, resolved)
	require.False(t, published)
}

func TestNativePADDropWriteGateBeforeSideEffects(t *testing.T) {
	for _, table := range []*planpb.TableDef{
		{Name: "n", Cols: []*planpb.ColDef{{Name: "s", Typ: planpb.Type{Id: int32(types.T_varchar), Charset: 3, CollationVersion: 1}}}},
		{Name: "v", ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view v as select 'A' collate utf8mb4_general_ci='a' as b","collation_binding_revision":1,"required_protocol_version":129}`}, Cols: []*planpb.ColDef{{Name: "b", Typ: types.T_bool.ToType().PlanType()}}},
	} {
		t.Run(table.Name, func(t *testing.T) {
			c, _ := expressionProtocolTestCompile(t)
			c.e = nil
			restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			drop := &planpb.DropTable{Table: table.Name, TableDef: table, IsView: table.ViewSql != nil}
			scope := &Scope{Proc: c.proc, Plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_DropTable{DropTable: drop}}}}}
			require.NotPanics(t, func() { require.ErrorContains(t, scope.DropTable(c), "protocol version 129") })
			// Multi DROP bypasses the scalar fast path; each member must still
			// reject before its own storage/lifecycle effects.
			scope.Plan.GetDdl().Definition = &planpb.DataDefinition_DropTable{DropTable: &planpb.DropTable{Tables: []*planpb.DropTable{
				drop, {Table: "legacy", TableDef: &planpb.TableDef{Name: "legacy"}},
			}}}
			require.NotPanics(t, func() { require.ErrorContains(t, scope.DropTable(c), "protocol version 129") })
		})
	}
}
