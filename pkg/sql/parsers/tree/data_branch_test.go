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
	stmt.OutputOpt = &DiffOutputOpt{Count: true}
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
