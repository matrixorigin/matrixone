// Copyright 2025 Matrix Origin
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

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataBranchCreateTableLifecycle(t *testing.T) {
	stmt := NewDataBranchCreateTable()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Data Branch Create Table", stmt.GetStatementType())
	require.Equal(t, "Data Branch Create Table", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
	require.Equal(t, DataBranch_CreateTable, stmt.DataBranchType())

	stmt.ToAccountOpt = &ToAccountOpt{}
	stmt.CreateTable.IfNotExists = true
	stmt.reset()
	require.Nil(t, stmt.ToAccountOpt)
	require.False(t, stmt.CreateTable.IfNotExists)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchDeleteTableLifecycle(t *testing.T) {
	stmt := NewDataBranchDeleteTable()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Data Branch Delete Table", stmt.GetStatementType())
	require.Equal(t, "Data Branch Delete Table", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
	require.Equal(t, DataBranch_DeleteTable, stmt.DataBranchType())

	stmt.TableName.ObjectName = Identifier("table")
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.TableName.ObjectName)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchCreateDatabaseLifecycle(t *testing.T) {
	stmt := NewDataBranchCreateDatabase()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Data Branch Create Database", stmt.GetStatementType())
	require.Equal(t, "Data Branch Create Database", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
	require.Equal(t, DataBranch_CreateDatabase, stmt.DataBranchType())

	stmt.reset()

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchDeleteDatabaseLifecycle(t *testing.T) {
	stmt := NewDataBranchDeleteDatabase()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "Data Branch Delete Database", stmt.GetStatementType())
	require.Equal(t, "Data Branch Delete Database", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())
	require.Equal(t, DataBranch_DeleteDatabase, stmt.DataBranchType())

	stmt.DatabaseName = Identifier("db")
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.DatabaseName)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchDiffLifecycle(t *testing.T) {
	stmt := NewDataBranchDiff()
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "branch diff", stmt.GetStatementType())
	require.Equal(t, "branch diff", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	stmt.TargetTable.ObjectName = Identifier("target")
	stmt.BaseTable.ObjectName = Identifier("base")
	stmt.OutputOpt = &DiffOutputOpt{Count: true, Summary: true}
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.TargetTable.ObjectName)
	require.Nil(t, stmt.OutputOpt)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchMergeLifecycle(t *testing.T) {
	stmt := NewDataBranchMerge()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "branch merge", stmt.GetStatementType())
	require.Equal(t, "branch merge", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	stmt.SrcTable.ObjectName = Identifier("src")
	stmt.DstTable.ObjectName = Identifier("dst")
	stmt.ConflictOpt = &ConflictOpt{Opt: CONFLICT_ACCEPT}
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.SrcTable.ObjectName)
	require.Nil(t, stmt.ConflictOpt)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestDataBranchPickLifecycle(t *testing.T) {
	stmt := NewDataBranchPick()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "branch pick", stmt.GetStatementType())
	require.Equal(t, "branch pick", stmt.String())
	require.Equal(t, "DataBranchPick", stmt.TypeName())

	stmt.SrcTable.ObjectName = Identifier("src")
	stmt.DstTable.ObjectName = Identifier("dst")
	stmt.Keys = &PickKeys{Type: PickKeysValues}
	stmt.ConflictOpt = &ConflictOpt{Opt: CONFLICT_SKIP}
	stmt.BetweenFrom = "sp1"
	stmt.BetweenTo = "sp2"
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.SrcTable.ObjectName)
	require.Nil(t, stmt.Keys)
	require.Nil(t, stmt.ConflictOpt)
	require.Empty(t, stmt.BetweenFrom)
}

func TestDataBranchPickFormat(t *testing.T) {
	makeTN := func(name string) TableName {
		var tn TableName
		tn.ObjectName = Identifier(name)
		return tn
	}

	// Basic KEYS with values
	stmt := &DataBranchPick{
		SrcTable: makeTN("src"),
		DstTable: makeTN("dst"),
		Keys: &PickKeys{
			Type: PickKeysValues,
			KeyExprs: []Expr{
				NewNumVal[int64](1, "1", false, P_int64),
				NewNumVal[int64](2, "2", false, P_int64),
			},
		},
	}
	ctx := NewFmtCtx(0)
	stmt.Format(ctx)
	require.Contains(t, ctx.String(), "data branch pick")
	require.Contains(t, ctx.String(), "src")
	require.Contains(t, ctx.String(), "into")
	require.Contains(t, ctx.String(), "dst")
	require.Contains(t, ctx.String(), "keys (")

	// With BETWEEN SNAPSHOT
	stmt2 := &DataBranchPick{
		SrcTable:    makeTN("src"),
		DstTable:    makeTN("dst"),
		BetweenFrom: "snap_start",
		BetweenTo:   "snap_end",
	}
	ctx = NewFmtCtx(0)
	stmt2.Format(ctx)
	result := ctx.String()
	require.Contains(t, result, "between snapshot")
	require.Contains(t, result, "snap_start")
	require.Contains(t, result, "snap_end")

	// With conflict options
	for _, tt := range []struct {
		opt    int
		expect string
	}{
		{CONFLICT_FAIL, "fail"},
		{CONFLICT_SKIP, "skip"},
		{CONFLICT_ACCEPT, "accept"},
	} {
		stmt3 := &DataBranchPick{
			SrcTable: makeTN("src"),
			DstTable: makeTN("dst"),
			Keys: &PickKeys{
				Type:     PickKeysValues,
				KeyExprs: []Expr{NewNumVal[int64](1, "1", false, P_int64)},
			},
			ConflictOpt: &ConflictOpt{Opt: tt.opt},
		}
		ctx = NewFmtCtx(0)
		stmt3.Format(ctx)
		require.Contains(t, ctx.String(), "when conflict "+tt.expect)
	}
}
