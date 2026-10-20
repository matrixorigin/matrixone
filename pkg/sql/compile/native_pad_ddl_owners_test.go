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

func TestNativePADIndependentDDLWriteGates(t *testing.T) {
	for _, operation := range []string{"create index", "drop index", "truncate", "rename", "clone source-only", "restore source-only"} {
		t.Run(operation, func(t *testing.T) {
			c, _ := expressionProtocolTestCompile(t)
			c.disableLock = true
			restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			table := &planpb.TableDef{Name: "n", Cols: []*planpb.ColDef{{Name: "s", Typ: planpb.Type{Id: int32(types.T_varchar), Charset: 3, CollationVersion: 1}}}}
			ctrl := gomock.NewController(t)
			eng, db, rel := mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockDatabase(ctrl), mock_frontend.NewMockRelation(ctrl)
			c.e = eng
			if operation == "drop index" || operation == "truncate" {
				eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(db, nil).AnyTimes()
				db.EXPECT().Relation(gomock.Any(), "n", gomock.Any()).Return(rel, nil).AnyTimes()
				rel.EXPECT().GetTableDef(gomock.Any()).Return(table).AnyTimes()
				rel.EXPECT().GetTableID(gomock.Any()).DoAndReturn(func(context.Context) uint64 {
					t.Fatal("unexpected lifecycle work after insufficient writer floor")
					return 1
				}).AnyTimes()
				rel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *planpb.TableDef {
					t.Fatal("unexpected constraint preparation after insufficient writer floor")
					return table
				}).AnyTimes()
			}
			ddl := &planpb.DataDefinition{}
			scope := &Scope{Proc: c.proc, Plan: &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: ddl}}}
			var run func() error
			switch operation {
			case "create index":
				// No engine calls are allowed before the writer gate.
				ddl.Definition = &planpb.DataDefinition_CreateIndex{CreateIndex: &planpb.CreateIndex{Database: "db", Table: "n", TableDef: table}}
				run = func() error { return scope.CreateIndex(c) }
			case "drop index":
				ddl.Definition = &planpb.DataDefinition_DropIndex{DropIndex: &planpb.DropIndex{Database: "db", Table: "n", IndexName: "k"}}
				run = func() error { return scope.DropIndex(c) }
			case "truncate":
				ddl.Definition = &planpb.DataDefinition_TruncateTable{TruncateTable: &planpb.TruncateTable{Database: "db", Table: "n"}}
				run = func() error { return scope.TruncateTable(c) }
			case "rename":
				ddl.Definition = &planpb.DataDefinition_RenameTable{RenameTable: &planpb.RenameTable{AlterTables: []*planpb.AlterTable{{Database: "db", TableDef: table}}}}
				run = func() error { return scope.RenameTable(c) }
			case "clone source-only", "restore source-only":
				clone := &planpb.CloneTable{SrcTableDef: table}
				ddl.Definition = &planpb.DataDefinition_CloneTable{CloneTable: clone}
				if operation == "clone source-only" {
					run = func() error { return scope.TableClone(c) }
				} else {
					run = func() error { return scope.RestoreTable(c, clone) }
				}
			}
			require.ErrorContains(t, run(), "protocol version 121")
		})
	}
}

func TestNativePADDatabaseDropIncludesBoolViews(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
	table := &planpb.TableDef{Name: "v", ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view v as select 'A' collate utf8mb4_general_ci='a' as b","collation_binding_revision":1,"required_protocol_version":121}`}}
	ctx := c.proc.Ctx
	err := c.dropDatabaseRelations(nil, []*planpb.DropTable{{Table: "v", TableDef: table, IsView: true}}, nil, true)
	require.ErrorContains(t, err, "protocol version 121")
	require.Same(t, ctx, c.proc.Ctx)

	ctrl := gomock.NewController(t)
	db, rel := mock_frontend.NewMockDatabase(ctrl), mock_frontend.NewMockRelation(ctrl)
	db.EXPECT().Relations(gomock.Any()).Return([]string{"v"}, nil)
	db.EXPECT().Relation(gomock.Any(), "v", gomock.Any()).Return(rel, nil)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(table)
	require.ErrorContains(t, c.requireDropDatabaseCollations(db, "db"), "protocol version 121")
	require.Same(t, ctx, c.proc.Ctx)
}

func TestNativePADDatabaseDropLegacyPreflight(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_then_legacy", true: "internal_error_is_not_missing"}[internal], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, _ := newDropDDLCompile(t, ctrl, nil)
			restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			ctx := c.proc.Ctx
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relations(gomock.Any()).Return([]string{"missing", "legacy"}, nil)
			lookupErr := moerr.NewNoSuchTable(ctx, "db", "missing")
			if internal {
				lookupErr = moerr.NewInternalError(ctx, "catalog lookup failed")
			}
			db.EXPECT().Relation(gomock.Any(), "missing", gomock.Any()).Return(nil, lookupErr)
			if !internal {
				rel := mock_frontend.NewMockRelation(ctrl)
				db.EXPECT().Relation(gomock.Any(), "legacy", gomock.Any()).Return(rel, nil)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{
					Name: "legacy", Cols: []*planpb.ColDef{{Name: "s", Typ: planpb.Type{Id: int32(types.T_varchar), Charset: 3}}},
				})
			}
			err := c.requireDropDatabaseCollations(db, "db")
			if internal {
				require.ErrorIs(t, err, lookupErr)
			} else {
				require.NoError(t, err)
			}
			require.Same(t, ctx, c.proc.Ctx)
		})
	}
}
