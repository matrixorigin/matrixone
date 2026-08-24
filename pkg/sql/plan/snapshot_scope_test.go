// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type namedSnapshotCompilerContext struct {
	*MockCompilerContext
	snapshot *Snapshot
}

func (c *namedSnapshotCompilerContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	return c.snapshot, nil
}

func snapshotScopeExprContainsObjectID(expr *planpb.Expr, objectID uint64) bool {
	literal := expr.GetLit()
	if literal != nil && (literal.GetI64Val() == int64(objectID) || literal.GetU64Val() == objectID) {
		return true
	}
	if function := expr.GetF(); function != nil {
		for _, argument := range function.Args {
			if snapshotScopeExprContainsObjectID(argument, objectID) {
				return true
			}
		}
	}
	return false
}

func TestValidateSnapshotScope(t *testing.T) {
	newSnapshot := func(level string, objectID uint64) *Snapshot {
		return &Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: level,
			ObjId: objectID,
		}}
	}

	tests := []struct {
		name       string
		snapshot   *Snapshot
		databaseID uint64
		tableID    uint64
		err        string
	}{
		{name: "timestamp snapshot", databaseID: 1, tableID: 2},
		{name: "cluster snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELCLUSTER.String(), 0), databaseID: 1, tableID: 2},
		{name: "account snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELACCOUNT.String(), 1), databaseID: 1, tableID: 2},
		{name: "database snapshot same database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1), databaseID: 1, tableID: 3},
		{name: "database snapshot other database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1), databaseID: 2, tableID: 3, err: "internal error: database-level snapshot(snapshot) does not belong to the database(db)"},
		{name: "table snapshot same table", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), databaseID: 1, tableID: 2},
		{name: "table snapshot other table", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), databaseID: 1, tableID: 3, err: "internal error: table-level snapshot(snapshot) does not belong to the table(db-table)"},
		{name: "unknown snapshot level", snapshot: newSnapshot("unknown", 1), databaseID: 1, tableID: 2, err: "internal error: unsupported snapshot level \"unknown\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSnapshotScope(test.snapshot, "db", "table", test.databaseID, test.tableID)
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}

func TestValidateSnapshotDatabaseScope(t *testing.T) {
	newSnapshot := func(level string, objectID uint64) *Snapshot {
		return &Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: level,
			ObjId: objectID,
		}}
	}

	tests := []struct {
		name     string
		snapshot *Snapshot
		err      string
	}{
		{name: "timestamp snapshot"},
		{name: "cluster snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELCLUSTER.String(), 0)},
		{name: "account snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELACCOUNT.String(), 1)},
		{name: "database snapshot same database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1)},
		{name: "database snapshot other database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 2), err: "internal error: database-level snapshot(snapshot) does not belong to the database(db)"},
		{name: "table snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), err: "internal error: table-level snapshot(snapshot) cannot read database-wide metadata for database(db)"},
		{name: "unknown snapshot level", snapshot: newSnapshot("unknown", 1), err: "internal error: unsupported snapshot level \"unknown\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSnapshotDatabaseScope(test.snapshot, "db", 1)
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}

func TestSnapshotTableID(t *testing.T) {
	require.Zero(t, SnapshotTableID(nil))
	require.Equal(t, uint64(2), SnapshotTableID(&planpb.TableDef{TblId: 2}))
	require.Equal(t, uint64(3), SnapshotTableID(&planpb.TableDef{TblId: 2, LogicalId: 3}))
}

func TestBuildShowDatabasesRejectsTableSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := NewMockCompilerContext2(ctrl)
	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42},
		Tenant: &planpb.SnapshotTenant{},
		ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: tree.SNAPSHOTLEVELTABLE.String(),
			ObjId: 7,
		},
	}
	ctx.EXPECT().GetAccountId().Return(uint32(0), nil)
	ctx.EXPECT().GetSnapshot().Return(nil)
	ctx.EXPECT().ResolveSnapshotWithSnapshotName("snapshot").Return(snapshot, nil)
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()

	_, err := buildShowDatabases(&tree.ShowDatabases{AtTsExpr: &tree.AtTimeStamp{
		Type:         tree.ATTIMESTAMPSNAPSHOT,
		SnapshotName: "snapshot",
		Expr:         tree.NewNumVal("snapshot", "snapshot", false, tree.P_char),
	}}, ctx)
	require.EqualError(t, err, "internal error: table-level snapshot(snapshot) cannot list databases")
}

func TestBuildShowDatabasesRestrictsDatabaseSnapshot(t *testing.T) {
	ctx := &namedSnapshotCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		snapshot: &Snapshot{
			TS:     &timestamp.Timestamp{PhysicalTime: 42},
			Tenant: &planpb.SnapshotTenant{},
			ExtraInfo: &planpb.SnapshotExtraInfo{
				Name:  "snapshot",
				Level: tree.SNAPSHOTLEVELDATABASE.String(),
				ObjId: 7,
			},
		},
	}
	ctx.tables["mo_database"].Cols = append(ctx.tables["mo_database"].Cols, &planpb.ColDef{
		Name: "dat_id",
		Typ:  planpb.Type{Id: int32(types.T_uint64)},
	})

	plan, err := buildShowDatabases(&tree.ShowDatabases{AtTsExpr: &tree.AtTimeStamp{
		Type:         tree.ATTIMESTAMPSNAPSHOT,
		SnapshotName: "snapshot",
		Expr:         tree.NewNumVal("snapshot", "snapshot", false, tree.P_char),
	}}, ctx)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.True(t, queryContainsExpr(plan.GetQuery(), func(expr *planpb.Expr) bool {
		return snapshotScopeExprContainsObjectID(expr, 7)
	}))
}

func TestCheckPrivilegeUsesSnapshotLogicalTableID(t *testing.T) {
	ctx := context.WithValue(
		context.Background(), tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelTable,
	)
	tableDef := &planpb.TableDef{
		DbName:    "db",
		Name:      "table",
		DbId:      1,
		TblId:     8,
		LogicalId: 7,
	}

	newSnapshot := func(objectID uint64) *Snapshot {
		return &Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: tree.SNAPSHOTLEVELTABLE.String(),
			ObjId: objectID,
		}}
	}

	require.NoError(t, checkPrivilege(
		ctx, 1, 1, nil, tableDef, "db", newSnapshot(7), tree.WithinDBCloneTable,
	))
	require.EqualError(t, checkPrivilege(
		ctx, 1, 1, nil, tableDef, "db", newSnapshot(8), tree.WithinDBCloneTable,
	), "internal error: table-level snapshot(snapshot) does not belong to the table(db-table)")
}
