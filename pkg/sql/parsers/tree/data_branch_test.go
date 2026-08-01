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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

	stmt.CreateTable.Table.ObjectName = Identifier("quote'dst")
	stmt.SrcTable.ObjectName = Identifier("quote'src")
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteIdentifier())
	stmt.Format(ctx)
	require.Equal(t, "data branch create table `quote'dst` from `quote'src`", ctx.String())
	require.Equal(t, "data branch create table", stmt.TypeName())

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

	stmt.TableName = makeDataBranchTableName("db`name", "table`name", nil)
	ctx := NewFmtCtx(dialect.MYSQL)
	stmt.Format(ctx)
	require.Equal(t, "data branch delete table `db``name`.`table``name`", ctx.String())
	require.Equal(t, "data branch delete table", stmt.TypeName())

	stmt.reset()
	require.Equal(t, Identifier(""), stmt.TableName.ObjectName)

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

	stmt.DstDatabase = Identifier("dst`db")
	stmt.SrcDatabase = Identifier("src`db")
	stmt.AtTsExpr = &AtTimeStamp{
		Type: ATTIMESTAMPSNAPSHOT,
		Expr: NewNumVal("snap'one", "snap'one", false, P_char),
	}
	stmt.ToAccountOpt = &ToAccountOpt{AccountName: Identifier("acc`name")}
	ctx := NewFmtCtx(dialect.MYSQL)
	stmt.Format(ctx)
	require.Equal(t, "data branch create database `dst``db` from `src``db`{snapshot = 'snap''one'} to account `acc``name`", ctx.String())
	require.Equal(t, "data branch create database", stmt.TypeName())

	stmt.reset()
	require.Empty(t, stmt.DstDatabase)
	require.Nil(t, stmt.AtTsExpr)

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

	stmt.DatabaseName = Identifier("db`name")
	ctx := NewFmtCtx(dialect.MYSQL)
	stmt.Format(ctx)
	require.Equal(t, "data branch delete database `db``name`", ctx.String())
	require.Equal(t, "data branch delete database", stmt.TypeName())

	stmt.reset()
	require.Equal(t, Identifier(""), stmt.DatabaseName)

	stmt.Free()
}

func TestDataBranchDiffLifecycle(t *testing.T) {
	stmt := NewDataBranchDiff()
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "branch diff", stmt.GetStatementType())
	require.Equal(t, "branch diff", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	stmt.TargetTable = makeDataBranchTableName("db", "target", nil)
	stmt.BaseTable = makeDataBranchTableName("db", "base", nil)
	stmt.Columns = IdentifierList{Identifier("id"), Identifier("select")}
	stmt.OutputOpt = &DiffOutputOpt{Count: true}
	ctx := NewFmtCtx(dialect.MYSQL)
	stmt.Format(ctx)
	require.Equal(t, "data branch diff `db`.`target` against `db`.`base` columns (`id`, `select`) output count", ctx.String())
	require.Equal(t, "data branch diff", stmt.TypeName())

	stmt.reset()
	require.Equal(t, Identifier(""), stmt.TargetTable.ObjectName)
	require.Nil(t, stmt.Columns)
	require.Nil(t, stmt.OutputOpt)

	stmt.Free()
}

func TestDataBranchMergeLifecycle(t *testing.T) {
	stmt := NewDataBranchMerge()
	require.NotNil(t, stmt)

	require.Equal(t, frontendStatusTyp, stmt.StmtKind())
	require.Equal(t, "branch merge", stmt.GetStatementType())
	require.Equal(t, "branch merge", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	stmt.SrcTable = makeDataBranchTableName("db", "src", nil)
	stmt.DstTable = makeDataBranchTableName("db", "dst", nil)
	stmt.ConflictOpt = &ConflictOpt{Opt: CONFLICT_ACCEPT}
	ctx := NewFmtCtx(dialect.MYSQL)
	stmt.Format(ctx)
	require.Equal(t, "data branch merge `db`.`src` into `db`.`dst` when conflict accept", ctx.String())
	require.Equal(t, "data branch merge", stmt.TypeName())

	stmt.reset()
	require.Equal(t, Identifier(""), stmt.SrcTable.ObjectName)
	require.Nil(t, stmt.ConflictOpt)

	stmt.Free()
}

func TestDataBranchDiffFormatOutputOptions(t *testing.T) {
	limit := int64(0)
	for _, test := range []struct {
		name   string
		option *DiffOutputOpt
		want   string
	}{
		{name: "none", want: "data branch diff `target` against `base`"},
		{name: "as", option: &DiffOutputOpt{As: makeDataBranchTableName("out", "result", nil)}, want: "data branch diff `target` against `base` output as `out`.`result`"},
		{name: "empty file", option: &DiffOutputOpt{}, want: "data branch diff `target` against `base` output file ''"},
		{name: "file", option: &DiffOutputOpt{DirPath: "/tmp/branch's/"}, want: "data branch diff `target` against `base` output file '/tmp/branch''s/'"},
		{name: "zero limit", option: &DiffOutputOpt{Limit: &limit}, want: "data branch diff `target` against `base` output limit 0"},
		{name: "count", option: &DiffOutputOpt{Count: true}, want: "data branch diff `target` against `base` output count"},
		{name: "summary", option: &DiffOutputOpt{Summary: true}, want: "data branch diff `target` against `base` output summary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt := &DataBranchDiff{
				TargetTable: makeDataBranchTableName("", "target", nil),
				BaseTable:   makeDataBranchTableName("", "base", nil),
				OutputOpt:   test.option,
			}
			require.Equal(t, test.want, String(stmt, dialect.MYSQL))
		})
	}
}

func TestDataBranchMergeFormatConflictOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		conflict *ConflictOpt
		want     string
	}{
		{name: "default", want: "data branch merge `src` into `dst`"},
		{name: "fail", conflict: &ConflictOpt{Opt: CONFLICT_FAIL}, want: "data branch merge `src` into `dst` when conflict fail"},
		{name: "skip", conflict: &ConflictOpt{Opt: CONFLICT_SKIP}, want: "data branch merge `src` into `dst` when conflict skip"},
		{name: "accept", conflict: &ConflictOpt{Opt: CONFLICT_ACCEPT}, want: "data branch merge `src` into `dst` when conflict accept"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt := &DataBranchMerge{
				SrcTable:    makeDataBranchTableName("", "src", nil),
				DstTable:    makeDataBranchTableName("", "dst", nil),
				ConflictOpt: test.conflict,
			}
			require.Equal(t, test.want, String(stmt, dialect.MYSQL))
		})
	}
}

func makeDataBranchTableName(database, table string, atTsExpr *AtTimeStamp) TableName {
	return *NewTableName(
		Identifier(table),
		ObjectNamePrefix{
			SchemaName:     Identifier(database),
			ExplicitSchema: database != "",
		},
		atTsExpr,
	)
}

func TestObjectListLifecycle(t *testing.T) {
	stmt := NewObjectList()
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "object list", stmt.GetStatementType())
	require.Equal(t, "object list", stmt.String())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	// Test setting fields
	stmt.Database = Identifier("testdb")
	stmt.Table = Identifier("testtable")
	stmt.Snapshot = Identifier("snap1")
	againstSnap := Identifier("snap2")
	stmt.AgainstSnapshot = &againstSnap

	require.Equal(t, Identifier("testdb"), stmt.Database)
	require.Equal(t, Identifier("testtable"), stmt.Table)
	require.Equal(t, Identifier("snap1"), stmt.Snapshot)
	require.NotNil(t, stmt.AgainstSnapshot)
	require.Equal(t, Identifier("snap2"), *stmt.AgainstSnapshot)

	// Test reset
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.Database)
	require.Equal(t, Identifier(""), stmt.Table)
	require.Equal(t, Identifier(""), stmt.Snapshot)
	require.Nil(t, stmt.AgainstSnapshot)

	require.Panics(t, func() {
		stmt.Format(nil)
	})
	require.Panics(t, func() {
		stmt.TypeName()
	})

	stmt.Free()
}

func TestGetObjectLifecycle(t *testing.T) {
	stmt := NewGetObject()
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "get object", stmt.GetStatementType())
	require.Equal(t, "get object", stmt.String())
	require.Equal(t, "get object", stmt.TypeName())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	// Test setting fields
	stmt.ObjectName = Identifier("obj_001")
	stmt.ChunkIndex = 5

	require.Equal(t, Identifier("obj_001"), stmt.ObjectName)
	require.Equal(t, int64(5), stmt.ChunkIndex)

	// Test Format
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GET OBJECT obj_001 OFFSET 5", ctx.String())

	// Test with ChunkIndex = -1 (metadata only)
	stmt.ChunkIndex = -1
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx2)
	require.Equal(t, "GET OBJECT obj_001 OFFSET -1", ctx2.String())

	// Test reset
	stmt.reset()
	require.Equal(t, Identifier(""), stmt.ObjectName)
	require.Equal(t, int64(0), stmt.ChunkIndex)

	stmt.Free()
}

func TestGetDdlLifecycle(t *testing.T) {
	stmt := NewGetDdl()
	require.NotNil(t, stmt)

	require.Equal(t, compositeResRowType, stmt.StmtKind())
	require.Equal(t, "get ddl", stmt.GetStatementType())
	require.Equal(t, "get ddl", stmt.String())
	require.Equal(t, "get ddl", stmt.TypeName())
	require.Equal(t, QueryTypeOth, stmt.GetQueryType())

	// Test Format with no fields set
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "GETDDL", ctx.String())

	// Test Format with Database only
	dbName := Identifier("testdb")
	stmt.Database = &dbName
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx2)
	require.Equal(t, "GETDDL DATABASE testdb", ctx2.String())

	// Test Format with Database and Table
	tableName := Identifier("testtable")
	stmt.Table = &tableName
	ctx3 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx3)
	require.Equal(t, "GETDDL DATABASE testdb TABLE testtable", ctx3.String())

	// Test Format with all fields
	snapName := Identifier("snap1")
	stmt.Snapshot = &snapName
	ctx4 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx4)
	require.Equal(t, "GETDDL DATABASE testdb TABLE testtable SNAPSHOT snap1", ctx4.String())

	// Test reset
	stmt.reset()
	require.Nil(t, stmt.Database)
	require.Nil(t, stmt.Table)
	require.Nil(t, stmt.Snapshot)

	stmt.Free()
}

func TestObjectListFormat_GoodPath(t *testing.T) {
	// Test basic ObjectList with only snapshot
	stmt := NewObjectList()
	require.NotNil(t, stmt)

	stmt.Snapshot = Identifier("snap1")
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt.Format(ctx)
	require.Equal(t, "OBJECTLIST SNAPSHOT snap1", ctx.String())

	// Test with Database
	stmt2 := NewObjectList()
	stmt2.Database = Identifier("testdb")
	stmt2.Snapshot = Identifier("snap2")
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt2.Format(ctx2)
	require.Equal(t, "OBJECTLIST DATABASE testdb SNAPSHOT snap2", ctx2.String())

	// Test with Database and Table
	stmt3 := NewObjectList()
	stmt3.Database = Identifier("testdb")
	stmt3.Table = Identifier("testtable")
	stmt3.Snapshot = Identifier("snap3")
	ctx3 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt3.Format(ctx3)
	require.Equal(t, "OBJECTLIST DATABASE testdb TABLE testtable SNAPSHOT snap3", ctx3.String())

	// Test with AgainstSnapshot
	stmt4 := NewObjectList()
	stmt4.Snapshot = Identifier("snap4")
	againstSnap := Identifier("against_snap")
	stmt4.AgainstSnapshot = &againstSnap
	ctx4 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt4.Format(ctx4)
	require.Equal(t, "OBJECTLIST SNAPSHOT snap4 AGAINST SNAPSHOT against_snap", ctx4.String())

	// Test with FROM clause (SubscriptionAccountName and PubName)
	stmt5 := NewObjectList()
	stmt5.Snapshot = Identifier("snap5")
	stmt5.SubscriptionAccountName = "sys"
	stmt5.PubName = Identifier("mypub")
	ctx5 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt5.Format(ctx5)
	require.Equal(t, "OBJECTLIST SNAPSHOT snap5 FROM sys PUBLICATION mypub", ctx5.String())

	// Test with all fields
	stmt6 := NewObjectList()
	stmt6.Database = Identifier("db1")
	stmt6.Table = Identifier("tbl1")
	stmt6.Snapshot = Identifier("snap6")
	againstSnap2 := Identifier("against6")
	stmt6.AgainstSnapshot = &againstSnap2
	stmt6.SubscriptionAccountName = "account1"
	stmt6.PubName = Identifier("pub1")
	ctx6 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	stmt6.Format(ctx6)
	require.Equal(t, "OBJECTLIST DATABASE db1 TABLE tbl1 SNAPSHOT snap6 AGAINST SNAPSHOT against6 FROM account1 PUBLICATION pub1", ctx6.String())
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
	require.Equal(t, "data branch pick `src` into `dst` keys (1, 2)", ctx.String())

	// With BETWEEN SNAPSHOT
	stmt2 := &DataBranchPick{
		SrcTable:    makeTN("src"),
		DstTable:    makeTN("dst"),
		BetweenFrom: "snap_start",
		BetweenTo:   "snap_end",
	}
	ctx = NewFmtCtx(0)
	stmt2.Format(ctx)
	require.Equal(t, "data branch pick `src` into `dst` between snapshot 'snap_start' and 'snap_end'", ctx.String())

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
		require.Equal(t, "data branch pick `src` into `dst` keys (1) when conflict "+tt.expect, ctx.String())
	}
}
