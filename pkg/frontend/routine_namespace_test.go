// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package frontend

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func namespaceRows(data ...[]interface{}) *MysqlResultSet {
	r := &MysqlResultSet{}
	for i := 0; i < 16; i++ {
		c := &MysqlColumn{}
		c.SetName(fmt.Sprint(i))
		c.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		if i < 4 {
			c.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		}
		r.AddColumn(c)
	}
	for _, row := range data {
		r.AddRow(row)
	}
	return r
}
func namespaceMember(id uint64, arg string) []interface{} {
	return []interface{}{uint64(41), id, uint64(1), uint64(1), arg, "body", "python", "bigint", "db", "", "INVOKER", arg, "body", "python", "bigint", "INVOKER"}
}
func TestRoutineNamespaceInvalidatesBetterOverload(t *testing.T) {
	input := []types.Type{types.T_int16.ToType()}
	oldMatch, oldCost := function.PythonUdfArgTypeMatch(input, []types.Type{types.T_int64.ToType()})
	newMatch, newCost := function.PythonUdfArgTypeMatch(input, input)
	require.True(t, oldMatch && newMatch)
	require.Less(t, newCost, oldCost)
	old := namespaceMember(41, "bigint")
	sibling := namespaceMember(42, "smallint")
	before, err := decodeRoutineNamespaces(context.Background(), namespaceRows(old))
	require.NoError(t, err)
	after, err := decodeRoutineNamespaces(context.Background(), namespaceRows(old, sibling))
	require.NoError(t, err)
	dependency := testRoutinePlanDependency()
	dependency.NamespaceFingerprint = before[41]
	state := testRoutinePlanState()
	state.namespaceFingerprint = before[41]
	require.False(t, routinePlanDependenciesChanged([]*planpb.RoutinePlanDependency{dependency}, map[uint64]routinePlanCatalogState{41: state}))
	state.namespaceFingerprint = after[41]
	require.True(t, routinePlanDependenciesChanged([]*planpb.RoutinePlanDependency{dependency}, map[uint64]routinePlanCatalogState{41: state}), "selected identity unchanged, but new better overload must invalidate")
	// Rollback/restoring the identical candidate state preserves the dependency.
	restored, err := decodeRoutineNamespaces(context.Background(), namespaceRows(old))
	require.NoError(t, err)
	require.Equal(t, before, restored)
	reordered, err := decodeRoutineNamespaces(context.Background(), namespaceRows(sibling, old))
	require.NoError(t, err)
	require.Equal(t, after, reordered)
	require.Equal(t, after[41], after[42], "all selected siblings reuse one namespace hash")
	// Drop and replace of an unselected member change the name dependency.
	sibling[2] = uint64(2)
	replaced, err := decodeRoutineNamespaces(context.Background(), namespaceRows(old, sibling))
	require.NoError(t, err)
	require.NotEqual(t, after, replaced)
	// Effective revision metadata, rather than only initial head projection.
	sibling[11] = "int"
	effective, err := decodeRoutineNamespaces(context.Background(), namespaceRows(old, sibling))
	require.NoError(t, err)
	require.NotEqual(t, replaced, effective)
	_, err = decodeRoutineNamespaces(context.Background(), namespaceRows(old, old))
	require.ErrorContains(t, err, "duplicate")
	data, err := proto.Marshal(dependency)
	require.NoError(t, err)
	var decoded planpb.RoutinePlanDependency
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.Equal(t, dependency.NamespaceFingerprint, decoded.NamespaceFingerprint)
	decoded.NamespaceFingerprint = ""
	require.ErrorContains(t, validateRoutinePlanDependencyShape(&decoded, 9), "incomplete")
}
func TestRoutineNamespaceQueryReadsAllLanguagesAndSignatures(t *testing.T) {
	query := routineNamespacesSQL([]uint64{41, 42}, nil)
	require.Contains(t, query, "f.db=selected.db and f.name=selected.name")
	require.Contains(t, query, "where function_id in (41,42) group by db,name")
	require.NotContains(t, query, "f.language =")
	require.NotContains(t, query, "f.arg_types =")
	require.Contains(t, query, "limit 65537")
}
func TestRoutineNamespaceReadIsBounded(t *testing.T) {
	oversized := namespaceMember(41, "bigint")
	oversized[5] = strings.Repeat("x", maxRoutineNamespaceBytes+1)
	_, err := decodeRoutineNamespaces(context.Background(), namespaceRows(oversized))
	require.ErrorContains(t, err, "namespace bytes")
}

type routineSnapshotWorkspace struct {
	client.Workspace
	advances int
}

func (w *routineSnapshotWorkspace) IncrStatementID(context.Context, bool) error {
	w.advances++
	return fmt.Errorf("snapshot advanced")
}

func TestRoutineCatalogReadsKeepRCSnapshotUntilFinish(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := &routineSnapshotWorkspace{}
	op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	op.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	back := &backExec{backSes: &backSession{}}
	restore := pinRoutineCatalogReads(back)
	// This is the real statement boundary that would advance an RC snapshot.
	// A concurrent DDL commit between these internal SELECTs cannot invoke it.
	for i := 0; i < 3; i++ {
		require.NoError(t, executeStmtWithIncrStmt(back.backSes, nil, nil, op))
	}
	require.Zero(t, workspace.advances)
	restore()
	require.False(t, back.backSes.IsDerivedStmt())
	err := executeStmtWithIncrStmt(back.backSes, nil, &ExecCtx{reqCtx: context.Background()}, op)
	require.ErrorContains(t, err, "snapshot advanced")
	require.Equal(t, 1, workspace.advances)
	// Nested shared readers restore the existing derived state rather than
	// committing/advancing their caller's statement on return.
	back.backSes.ReplaceDerivedStmt(true)
	restore = pinRoutineCatalogReads(back)
	restore()
	require.True(t, back.backSes.IsDerivedStmt())
}

type namespaceValidationSession struct{ FeSession }

func (*namespaceValidationSession) GetTxnCompileCtx() *TxnCompilerContext { return nil }
func (*namespaceValidationSession) GetAccountId() uint32                  { return 9 }
func TestRoutinePlanWithoutNamespaceRebinds(t *testing.T) {
	dependency := testRoutinePlanDependency()
	dependency.NamespaceFingerprint = ""
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{RoutineDependencies: []*planpb.RoutinePlanDependency{dependency}}}}
	changed, err := validateRoutinePlanDependencies(context.Background(), &namespaceValidationSession{}, p)
	require.NoError(t, err)
	require.True(t, changed)
}

func TestRoutineCatalogDatabaseUsesSnapshotTenant(t *testing.T) {
	snapshot := &planpb.Snapshot{Tenant: &planpb.SnapshotTenant{TenantID: 7}}
	require.Equal(t, uint32(7), routineCatalogAccountID(0, snapshot))
	require.Equal(t, uint32(9), routineCatalogAccountID(9, nil))
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	bh.sql2result["select dat_id from mo_catalog.mo_database where datname = 'source_db' and account_id = 7;"] = singleInt64Result("dat_id", 88)
	ctx := defines.AttachAccountId(context.Background(), 7)
	id, err := routineCatalogDatabaseID(ctx, bh, "source_db", routineCatalogAccountID(0, snapshot), snapshot)
	require.NoError(t, err)
	require.Equal(t, uint64(88), id)
	require.Contains(t, bh.executedSqls[0], "account_id = 7")
}
