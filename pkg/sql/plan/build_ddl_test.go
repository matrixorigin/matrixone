// Copyright 2021 - 2022 Matrix Origin
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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type rootSQLCompilerContext struct {
	*MockCompilerContext
	rootSQL string
	calls   int
}

type viewReplacementCompilerContext struct {
	*rootSQLCompilerContext
	building            bool
	database            string
	view                string
	historicalSnapshot  *Snapshot
	timestampValid      bool
	lowerCaseTableNames int64
}

func (c *viewReplacementCompilerContext) SetBuildingAlterView(building bool, database, view string) {
	c.building = building
	c.database = database
	c.view = view
}

func (c *viewReplacementCompilerContext) GetBuildingAlterView() (bool, string, string) {
	return c.building, c.database, c.view
}

func (c *viewReplacementCompilerContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	return c.historicalSnapshot, nil
}

func (c *viewReplacementCompilerContext) CheckTimeStampValid(int64) (bool, error) {
	return c.timestampValid, nil
}

func (c *viewReplacementCompilerContext) GetLowerCaseTableNames() int64 {
	return c.lowerCaseTableNames
}

type viewReplacementTxnOperator struct {
	client.TxnOperator
	snapshotTS timestamp.Timestamp
}

func (o viewReplacementTxnOperator) Txn() txnpb.TxnMeta {
	return txnpb.TxnMeta{SnapshotTS: o.snapshotTS}
}

type captureSQLExecutor struct {
	exec func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e *captureSQLExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	return e.exec(ctx, sql, opts)
}

func (e *captureSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	return moerr.NewInternalErrorNoCtx("unexpected ExecTxn")
}

type autoIncrementOffsetCompilerContext struct {
	*MockCompilerContext
	offset int64
}

type subscriptionScopeCompilerContext struct {
	*MockCompilerContext
	subscription  *SubscriptionMeta
	querying      *SubscriptionMeta
	publisherByID map[uint64]*TableDef
}

func (c *subscriptionScopeCompilerContext) SetQueryingSubscription(meta *SubscriptionMeta) {
	c.querying = meta
}

func (c *subscriptionScopeCompilerContext) GetQueryingSubscription() *SubscriptionMeta {
	return c.querying
}

func (c *subscriptionScopeCompilerContext) GetSubscriptionMeta(
	dbName string,
	_ *Snapshot,
) (*SubscriptionMeta, error) {
	if dbName == c.subscription.SubName {
		return c.subscription, nil
	}
	if c.querying != nil && dbName != c.querying.SubName {
		publisherBinding := *c.querying
		publisherBinding.DbName = dbName
		return &publisherBinding, nil
	}
	return nil, nil
}

func (c *subscriptionScopeCompilerContext) ResolveSubscriptionTableById(
	tableID uint64,
	_ *SubscriptionMeta,
) (*ObjectRef, *TableDef, error) {
	tableDef := DeepCopyTableDef(c.publisherByID[tableID], true)
	if tableDef == nil {
		return nil, nil, nil
	}
	return &ObjectRef{SchemaName: tableDef.DbName, ObjName: tableDef.Name}, tableDef, nil
}

func (c *autoIncrementOffsetCompilerContext) ResolveVariable(
	varName string, isSystemVar, isGlobalVar bool,
) (interface{}, error) {
	if varName == "auto_increment_offset" {
		return c.offset, nil
	}
	return c.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

func TestBuildRenameTableUsesPriorDestinationAsNextSource(t *testing.T) {
	stmt, err := parsers.ParseOne(
		t.Context(),
		dialect.MYSQL,
		"rename table t1 to t2, t2 to t3",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	delete(ctx.tables, "t2")
	delete(ctx.tables, "t3")
	delete(ctx.objects, "t2")
	delete(ctx.objects, "t3")
	ctx.tables["t1"] = DeepCopyTableDef(ctx.tables["nation"], true)
	ctx.tables["t1"].Name = "t1"
	ctx.objects["t1"] = &ObjectRef{SchemaName: "tpch", ObjName: "t1"}

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	renames := p.GetDdl().GetRenameTable().GetAlterTables()
	require.Len(t, renames, 2)
	require.Equal(t, "t1", renames[0].GetActions()[0].GetAlterName().GetOldName())
	require.Equal(t, "t2", renames[0].GetActions()[0].GetAlterName().GetNewName())
	require.Equal(t, "t2", renames[1].GetTableDef().GetName())
	require.Equal(t, "t2", renames[1].GetActions()[0].GetAlterName().GetOldName())
	require.Equal(t, "t3", renames[1].GetActions()[0].GetAlterName().GetNewName())
}

func TestBuildTableRenameIdentifierLength(t *testing.T) {
	validName := "表" + strings.Repeat("a", MaxIdentifierLength-1)

	testCases := []struct {
		name string
		sql  func(string) string
	}{
		{
			name: "rename table",
			sql: func(name string) string {
				return fmt.Sprintf("rename table nation to `%s`", name)
			},
		},
		{
			name: "alter table rename",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation rename to `%s`", name)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name+" accepts 64 characters", func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, testCase.sql(validName))
			require.NoError(t, err)
		})

		invalidNames := []struct {
			name string
			make func(*MockOptimizer) string
		}{
			{
				name: "65 characters",
				make: func(*MockOptimizer) string {
					return "表" + strings.Repeat("b", MaxIdentifierLength)
				},
			},
			{
				name: "generated temporary table prefix",
				make: func(mock *MockOptimizer) string {
					return defines.GenTempTableName(
						mock.ctxt.GetProcess().GetSessionInfo().SessionId,
						"database",
						strings.Repeat("t", MaxIdentifierLength),
					)
				},
			},
		}

		for _, invalidName := range invalidNames {
			t.Run(testCase.name+" rejects "+invalidName.name, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				_, err := runOneStmt(mock, t, testCase.sql(invalidName.make(mock)))
				require.Error(t, err)
				moErr, ok := err.(*moerr.Error)
				require.True(t, ok, "unexpected error type %T: %v", err, err)
				require.Equal(t, moerr.ErrTooLongIdent, moErr.ErrorCode())
				require.Equal(t, uint16(moerr.ER_TOO_LONG_IDENT), moErr.MySQLCode())
			})
		}
	}
}

func TestBuildRejectsCrossDatabaseTableRename(t *testing.T) {
	testCases := []struct {
		name        string
		sql         string
		wantErrCode uint16
	}{
		{
			name:        "rename table changes database and name",
			sql:         "rename table tpch.nation to other.renamed",
			wantErrCode: moerr.ErrNotSupported,
		},
		{
			name:        "rename table changes only database",
			sql:         "rename table tpch.nation to other.nation",
			wantErrCode: moerr.ErrNotSupported,
		},
		{
			name:        "alter table changes database and name",
			sql:         "alter table tpch.nation rename to other.renamed",
			wantErrCode: moerr.ErrNotSupported,
		},
		{
			name:        "alter table changes only database",
			sql:         "alter table tpch.nation rename to other.nation",
			wantErrCode: moerr.ErrNotSupported,
		},
		{
			name:        "rename table resolves source before rejecting target database",
			sql:         "rename table tpch.missing_table to other.renamed",
			wantErrCode: moerr.ErrNoSuchTable,
		},
		{
			name:        "alter table resolves source before rejecting target database",
			sql:         "alter table tpch.missing_table rename to other.renamed",
			wantErrCode: moerr.ErrNoSuchTable,
		},
		{
			name: "rename table keeps explicit database",
			sql:  "rename table tpch.nation to tpch.renamed",
		},
		{
			name: "rename table inherits source database",
			sql:  "rename table tpch.nation to renamed",
		},
		{
			name: "alter table keeps explicit database",
			sql:  "alter table tpch.nation rename to tpch.renamed",
		},
		{
			name: "alter table inherits source database",
			sql:  "alter table tpch.nation rename to renamed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, testCase.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
			if testCase.wantErrCode != 0 {
				require.True(t, moerr.IsMoErrCode(err, testCase.wantErrCode), err)
				if testCase.wantErrCode == moerr.ErrNotSupported {
					require.Contains(t, err.Error(), "cross-database table rename")
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, p)
		})
	}
}

func TestBuildCreateTablePreservesTextCharset(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		want      uint32
		wantTable uint32
	}{
		{
			name:      "default text collation",
			sql:       "create table t(name varchar(10))",
			want:      uint32(types.CharsetUTF8),
			wantTable: uint32(types.CharsetUTF8),
		},
		{
			name: "table binary collation",
			sql: "create table t(name varchar(10)) character set utf8mb4 " +
				"collate utf8mb4_bin",
			want:      uint32(types.CharsetUTF8MB4Bin),
			wantTable: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name: "table binary collation before charset",
			sql: "create table t(name varchar(10)) collate utf8mb4_bin " +
				"character set utf8mb4",
			want:      uint32(types.CharsetUTF8MB4Bin),
			wantTable: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name:      "column binary collation",
			sql:       "create table t(name varchar(10) collate utf8mb4_bin)",
			want:      uint32(types.CharsetUTF8MB4Bin),
			wantTable: uint32(types.CharsetUTF8),
		},
		{
			name: "column collation overrides table",
			sql: "create table t(name varchar(10) collate utf8mb4_general_ci) " +
				"collate utf8mb4_bin",
			want:      uint32(types.CharsetUTF8),
			wantTable: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name: "column charset overrides table collation",
			sql: "create table t(name varchar(10) character set utf8mb4) " +
				"collate utf8mb4_bin",
			want:      uint32(types.CharsetUTF8),
			wantTable: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name: "column collation overrides binary table charset",
			sql: "create table t(name varchar(10) collate utf8mb4_general_ci) " +
				"character set binary",
			want:      uint32(types.CharsetUTF8),
			wantTable: uint32(types.CharsetBinary),
		},
		{
			name: "column charset overrides binary table charset",
			sql: "create table t(name varchar(10) character set utf8mb4) " +
				"character set binary",
			want:      uint32(types.CharsetUTF8),
			wantTable: uint32(types.CharsetBinary),
		},
		{
			name: "column collation wins independent of option order",
			sql: "create table t(name varchar(10) collate utf8mb4_bin " +
				"character set utf8mb4)",
			want:      uint32(types.CharsetUTF8MB4Bin),
			wantTable: uint32(types.CharsetUTF8),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.NoError(t, err)
			tableDef := p.GetDdl().GetCreateTable().GetTableDef()
			cols := tableDef.GetCols()
			require.NotEmpty(t, cols)
			require.Equal(t, int32(types.T_varchar), cols[0].Typ.Id)
			require.Equal(t, tc.want, cols[0].Typ.Charset)
			require.Equal(t, tc.wantTable, tableDef.DefaultCharset)
		})
	}
}

func TestBuildCreateTableRejectsUnsupportedCollations(t *testing.T) {
	for _, sql := range []string{
		"create table t(v varchar(8)) collate utf8mb4_de_pb_0900_ai_ci",
		"create table t(v varchar(8)) collate utf8mb4_unicode_ci",
		"create table t(v varchar(8) collate utf8mb4_0900_bin)",
		"create table t(v varchar(8) collate utf8_unicode_ci)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.ErrorContains(t, err, "unsupported collation")
		})
	}
}

func TestBuildCreateTableAcceptsMySQL8DefaultCollationCompatibilityAlias(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, `
		create table t_charset_mix (
			id bigint not null auto_increment,
			c_utf8mb4_ci varchar(100) character set utf8mb4 collate utf8mb4_0900_ai_ci null,
			c_utf8mb4_bin varchar(100) character set utf8mb4 collate utf8mb4_bin null,
			c_utf8mb4_general varchar(100) character set utf8mb4 collate utf8mb4_general_ci null,
			c_latin1 varchar(100) character set latin1 collate latin1_swedish_ci null,
			c_ascii varchar(100) character set ascii collate ascii_general_ci null,
			c_binary varbinary(100) null,
			primary key (id)
		) engine=InnoDB default charset=utf8mb4`, 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, uint32(types.CharsetUTF8), tableDef.DefaultCharset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(tableDef.Cols, "c_utf8mb4_ci").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), FindColumn(tableDef.Cols, "c_utf8mb4_bin").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(tableDef.Cols, "c_utf8mb4_general").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(tableDef.Cols, "c_latin1").Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), FindColumn(tableDef.Cols, "c_ascii").Typ.Charset)
	require.Equal(t, uint32(types.CharsetBinary), FindColumn(tableDef.Cols, "c_binary").Typ.Charset)

	showSQL, _, err := ConstructCreateTableSQL(ctx, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.NotContains(t, showSQL, "0900")
	require.Contains(t, showSQL, "COLLATE utf8mb4_bin")
}

func TestUnsupportedLegacyCollationExplainsDumpReplacement(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(v varchar(8)) collate utf8mb4_unicode_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()

	_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.ErrorContains(t, err,
		"replace it with 'utf8mb4_general_ci' when restoring legacy MatrixOne DDL")
}

func TestCreateTableInheritsEffectiveServerCollation(t *testing.T) {
	mock := NewMockCompilerContext(false)
	mock.ResolveVariableFunc = func(name string, isSystem, isGlobal bool) (interface{}, error) {
		if name == "collation_server" && isSystem && !isGlobal {
			return "utf8mb4_bin", nil
		}
		return nil, nil
	}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(v varchar(8))", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(mock, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), tableDef.DefaultCharset)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), tableDef.Cols[0].Typ.Charset)
}

func TestBuildCreateTableCharacterSetBinaryConvertsStringTypes(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(c char(4) character set binary, "+
			"v varchar(8) character set binary, x text character set binary)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 3)
	require.Equal(t, int32(types.T_binary), cols[0].Typ.Id)
	require.Equal(t, int32(types.T_varbinary), cols[1].Typ.Id)
	require.Equal(t, int32(types.T_blob), cols[2].Typ.Id)
	for _, col := range cols[:3] {
		require.Equal(t, uint32(types.CharsetBinary), col.Typ.Charset)
	}
}

func TestBuildCreateTableBinaryDefaultConvertsUnqualifiedStringType(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(v varchar(8)) character set binary", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, uint32(types.CharsetBinary), tableDef.DefaultCharset)
	require.Equal(t, int32(types.T_varbinary), tableDef.Cols[0].Typ.Id)
	require.Equal(t, uint32(types.CharsetBinary), tableDef.Cols[0].Typ.Charset)
}

func TestBuildCreateTableRejectsIncompatibleCharsetAndCollation(t *testing.T) {
	for _, sql := range []string{
		"create table t(v varchar(8)) character set utf8mb4 collate binary",
		"create table t(v varchar(8) character set binary collate utf8mb4_bin)",
		"create table t(v varchar(8)) character set latin1 collate ascii_general_ci",
		"create table t(v varchar(8) character set ascii collate latin1_swedish_ci)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.ErrorContains(t, err, "is not valid for CHARACTER SET")
		})
	}
}

func TestBuildCreateTableAcceptsUTF8MB3Aliases(t *testing.T) {
	for _, sql := range []string{
		"create table t(v varchar(8)) character set utf8 collate utf8mb3_bin",
		"create table t(v varchar(8) character set utf8mb3 collate utf8_general_ci)",
		"create table t(v varchar(8)) character set utf8 collate utf8mb4_general_ci",
		"create table t(v varchar(8) character set utf8 collate utf8mb4_bin)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.NoError(t, err)
		})
	}
}

func TestBuildCreateTableAcceptsSingleByteCharsetCompatibilityAliases(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		wantTable uint32
	}{
		{
			name: "latin1 column",
			sql: "create table t(v varchar(8) character set latin1 " +
				"collate latin1_swedish_ci)",
			wantTable: uint32(types.CharsetUTF8),
		},
		{
			name: "ascii column case insensitive spelling",
			sql: "create table t(v varchar(8) character set ASCII " +
				"collate ASCII_GENERAL_CI)",
			wantTable: uint32(types.CharsetUTF8),
		},
		{
			name:      "latin1 table default",
			sql:       "create table t(v varchar(8)) character set latin1 collate latin1_swedish_ci",
			wantTable: uint32(types.CharsetUTF8),
		},
		{
			name:      "ascii table default",
			sql:       "create table t(v varchar(8)) character set ascii collate ascii_general_ci",
			wantTable: uint32(types.CharsetUTF8),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.NoError(t, err)
			tableDef := p.GetDdl().GetCreateTable().GetTableDef()
			require.Equal(t, tc.wantTable, tableDef.DefaultCharset)
			require.Equal(t, uint32(types.CharsetUTF8), tableDef.Cols[0].Typ.Charset)
		})
	}
}

func TestBuildDropTemporaryTableOnlyTargetsTemporaryTable(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "drop temporary table nation", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	_, err = BuildPlan(ctx, stmt, false)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))

	ctx.tables["nation"].IsTemporary = true
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.True(t, p.GetDdl().GetDropTable().GetTableDef().GetIsTemporary())
	require.Empty(t, p.GetDdl().GetDropTable().GetUpdateFkSqls())
}

func TestBuildDropTemporaryTableIfExistsDoesNotTargetPermanentTable(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "drop temporary table if exists nation", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	require.Nil(t, p.GetDdl().GetDropTable().GetTableDef())
}

func TestBuildDropViewIfExistsDoesNotTargetBaseTable(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "drop view if exists nation", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	drop := p.GetDdl().GetDropTable()
	require.Empty(t, drop.GetTable())
	require.True(t, drop.GetIsView())
}

func TestBuildDropViewRejectsBaseTableWithoutIfExists(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "drop view nation", 1)
	require.NoError(t, err)
	defer stmt.Free()

	_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadView), err)
}

func TestBuildTruncateTemporaryTableDoesNotTargetPermanentTable(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepare=%t", prepare), func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table nation", 1)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := NewMockCompilerContext(false)
			ctx.tables["nation"].IsTemporary = true

			_, err = BuildPlan(ctx, stmt, prepare)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
			require.Equal(t, "no such table tpch.nation", err.Error())
		})
	}
}

func TestBuildTruncateTableSkipsSelfReferenceMarker(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table tree", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	ctx.tables["tree"] = &TableDef{
		Name:         "tree",
		TblId:        42,
		TableType:    catalog.SystemOrdinaryRel,
		RefChildTbls: []uint64{0},
		Fkeys: []*plan.ForeignKeyDef{
			{Name: "fk_self", ForeignTbl: 0},
			{Name: "fk_parent", ForeignTbl: 99},
		},
	}
	ctx.objects["tree"] = &ObjectRef{SchemaName: "tpch", ObjName: "tree"}

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, []uint64{99}, p.GetDdl().GetTruncateTable().GetForeignTbl())
}

func TestBuildTruncateMongoDBExternalTableRejectsReadOnlyDML(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.objects["mongo_events"] = &ObjectRef{
		SchemaName: "tpch",
		ObjName:    "mongo_events",
	}
	ctx.tables["mongo_events"] = &TableDef{
		Name:        "mongo_events",
		TableType:   catalog.SystemExternalRel,
		FeatureFlag: features.MongoDBExternal,
		Createsql: sqlmongodb.BuildCreateSQLEnvelope(sqlmongodb.TableMapping{
			Connection: "source",
			Database:   "telemetry",
			Collection: "events",
			SchemaMode: sqlmongodb.SchemaExplicit,
			Conversion: sqlmongodb.ConversionStrict,
			Columns: []sqlmongodb.ColumnMapping{
				{Name: "id", Path: "_id", TypeID: int32(types.T_varchar), Width: 64},
			},
		}),
		Cols: []*ColDef{
			{Name: "id", Typ: Type{Id: int32(types.T_varchar), Width: 64}},
		},
	}

	for _, prepare := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepare=%t", prepare), func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table mongo_events", 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(ctx, stmt, prepare)
			require.Nil(t, p)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
			require.Equal(t, "invalid input: cannot insert/update/delete from external table", err.Error())
		})
	}
}

func TestBuildTruncateNonMongoExternalTableKeepsExistingBehavior(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table external_events", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	ctx.objects["external_events"] = &ObjectRef{SchemaName: "tpch", ObjName: "external_events"}
	ctx.tables["external_events"] = &TableDef{
		Name:      "external_events",
		TableType: catalog.SystemExternalRel,
	}

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.NotNil(t, p.GetDdl().GetTruncateTable())
}

func TestBuildTruncateMalformedMongoDBExternalTableReturnsCatalogError(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table mongo_events", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	ctx.objects["mongo_events"] = &ObjectRef{SchemaName: "tpch", ObjName: "mongo_events"}
	ctx.tables["mongo_events"] = &TableDef{
		Name:        "mongo_events",
		TableType:   catalog.SystemExternalRel,
		FeatureFlag: features.MongoDBExternal,
	}

	p, err := BuildPlan(ctx, stmt, false)
	require.Nil(t, p)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
	require.Equal(t, "invalid input: MongoDB external table is missing its catalog envelope", err.Error())
}

func TestBuildAlterRenameColumnCarriesRewrittenChecks(t *testing.T) {
	stmt, err := parsers.ParseOne(
		t.Context(),
		dialect.MYSQL,
		"alter table nation rename column n_nationkey to nation_id",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false)
	ctx.tables["nation"].Checks = []*plan.CheckDef{{
		Name:      "ck_nationkey",
		OriginSql: "`n_nationkey` >= 0",
	}}

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	alter := p.GetDdl().GetAlterTable()
	require.Equal(t, plan.AlterTable_INPLACE, alter.GetAlgorithmType())
	require.Equal(t, "`nation_id` >= 0", alter.GetCopyTableDef().GetChecks()[0].GetOriginSql())
}

func TestBuildAlterRenameColumnRewritesComplexChecks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		check string
		want  string
	}{
		{
			name:  "searched case",
			check: "case when `n_nationkey` > 0 then 1 else 0 end = 1",
			want:  "case when `nation_id` > 0 then 1 else 0 end = 1",
		},
		{
			name:  "case without else",
			check: "case `n_nationkey` when 1 then 1 end = 1",
			want:  "case `nation_id` when 1 then 1 end = 1",
		},
		{
			name:  "fulltext match",
			check: "match (`n_nationkey`) against ('1')",
			want:  "MATCH (`nation_id`) AGAINST ('1')",
		},
		{
			name:  "like escape expression",
			check: "`n_name` like 'a!%' escape `n_nationkey`",
			want:  "`n_name` like 'a!%' escape `nation_id`",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(
				t.Context(),
				dialect.MYSQL,
				"alter table nation rename column n_nationkey to nation_id",
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := NewMockCompilerContext(false)
			ctx.tables["nation"].Checks = []*plan.CheckDef{{
				Name:      "ck_case",
				OriginSql: tc.check,
			}}

			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			checks := p.GetDdl().GetAlterTable().GetCopyTableDef().GetChecks()
			require.Len(t, checks, 1)
			require.Equal(t, tc.want, checks[0].GetOriginSql())
		})
	}
}

func TestBuildAlterRenameColumnRecoversLegacyChecks(t *testing.T) {
	parseRename := func(t *testing.T) tree.Statement {
		t.Helper()
		stmt, err := parsers.ParseOne(
			t.Context(),
			dialect.MYSQL,
			"alter table nation rename column n_nationkey to nation_id",
			1,
		)
		require.NoError(t, err)
		return stmt
	}

	for _, tc := range []struct {
		name      string
		sql       string
		algorithm plan.AlterTable_AlgorithmType
	}{
		{
			name:      "inplace",
			sql:       "alter table nation rename column n_nationkey to nation_id",
			algorithm: plan.AlterTable_INPLACE,
		},
		{
			name:      "copy",
			sql:       "alter table nation rename column n_nationkey to nation_id, algorithm=copy",
			algorithm: plan.AlterTable_COPY,
		},
	} {
		t.Run("unambiguous legacy check is recovered and rewritten "+tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := NewMockCompilerContext(false)
			ctx.tables["nation"].Checks = nil
			ctx.tables["nation"].Createsql = "create table nation(" +
				"n_nationkey int, constraint ck_nationkey check (n_nationkey >= 0))"

			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.Equal(t, tc.algorithm, alter.GetAlgorithmType())
			require.Len(t, alter.GetCopyTableDef().GetChecks(), 1)
			require.Equal(t, "ck_nationkey", alter.GetCopyTableDef().GetChecks()[0].GetName())
			require.Equal(t, "`nation_id` >= 0", alter.GetCopyTableDef().GetChecks()[0].GetOriginSql())
			require.Empty(t, ctx.tables["nation"].Checks, "catalog-owned source must remain unchanged")
		})
	}

	t.Run("legacy check inplace is rejected before protocol version 15", func(t *testing.T) {
		stmt := parseRename(t)
		defer stmt.Free()

		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Checks = nil
		ctx.tables["nation"].Createsql = "create table nation(" +
			"n_nationkey int, constraint ck_nationkey check (n_nationkey >= 0))"

		proc := ctx.GetProcess()
		rt := moruntime.ServiceRuntime(proc.GetService())
		original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		defer func() {
			if hadOriginal {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion14)

		_, err := BuildPlan(ctx, stmt, false)
		require.ErrorContains(t, err, "protocol version 15")
		require.Empty(t, ctx.tables["nation"].Checks, "catalog-owned source must remain unchanged")
	})

	t.Run("legacy check copy remains compatible at protocol version 14", func(t *testing.T) {
		stmt, err := parsers.ParseOne(
			t.Context(),
			dialect.MYSQL,
			"alter table nation rename column n_nationkey to nation_id, algorithm=copy",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Checks = nil
		ctx.tables["nation"].Createsql = "create table nation(" +
			"n_nationkey int, constraint ck_nationkey check (n_nationkey >= 0))"

		proc := ctx.GetProcess()
		rt := moruntime.ServiceRuntime(proc.GetService())
		original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		defer func() {
			if hadOriginal {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion14)

		p, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.Equal(t, plan.AlterTable_COPY, alter.GetAlgorithmType())
		require.Equal(t, "`nation_id` >= 0", alter.GetCopyTableDef().GetChecks()[0].GetOriginSql())
		require.Empty(t, ctx.tables["nation"].Checks, "catalog-owned source must remain unchanged")
	})

	t.Run("ambiguous legacy SQL mode is rejected", func(t *testing.T) {
		stmt := parseRename(t)
		defer stmt.Free()

		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Checks = nil
		ctx.tables["nation"].Createsql = `create table nation(
			n_nationkey int, n_name varchar(25), check (n_name = 'a\nb'))`

		_, err := BuildPlan(ctx, stmt, false)
		require.ErrorContains(t, err, "ambiguous SQL mode")
	})

	t.Run("multiple renames ignore CHECK text outside constraints", func(t *testing.T) {
		stmt, err := parsers.ParseOne(
			t.Context(),
			dialect.MYSQL,
			"alter table nation rename column n_nationkey to nation_id, "+
				"rename column n_name to nation_name",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()

		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Checks = nil
		ctx.tables["nation"].Createsql = "create table fk_foreign_key_checks4.nation(" +
			"n_nationkey int primary key, n_name varchar(25), " +
			"n_regionkey int, n_comment varchar(152))"

		p, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		copyDef := p.GetDdl().GetAlterTable().GetCopyTableDef()
		require.NotNil(t, FindColumn(copyDef.Cols, "nation_id"))
		require.NotNil(t, FindColumn(copyDef.Cols, "nation_name"))
		require.Empty(t, copyDef.Checks)
	})
}

func (c *rootSQLCompilerContext) GetRootSql() string {
	c.calls++
	return c.rootSQL
}

func TestBuildCreateOrReplaceViewRejectsRecursiveDefinition(t *testing.T) {
	recentTimestamp := time.Now().UTC().Add(-time.Minute).Format("2006-01-02 15:04:05.999999999")
	aheadOfWallClock := time.Now().Add(time.Minute)
	currentTxnSnapshot := timestamp.Timestamp{PhysicalTime: time.Now().Add(2 * time.Minute).UnixNano()}
	newViewDef := func(t *testing.T, sql, defaultDatabase string, lowerCaseTableNames *int64) *plan.TableDef {
		t.Helper()
		viewData, err := json.Marshal(ViewData{
			Stmt:                sql,
			DefaultDatabase:     defaultDatabase,
			SecurityType:        "DEFINER",
			LowerCaseTableNames: lowerCaseTableNames,
		})
		require.NoError(t, err)
		return &plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql:   &plan.ViewDef{View: string(viewData)},
		}
	}

	lctn0 := int64(0)
	lctn2 := int64(2)
	// AS OF TIMESTAMP is parsed in the MACHINE's zone — doResolveTimeStamp uses
	// time.LoadLocation("Local") — and converted with UnixNano, so a fixed
	// literal is not timezone-independent. '2262-04-11 23:47:16' sits on the
	// int64-nanosecond ceiling (2262-04-11 23:47:16.854775807 UTC): west of UTC
	// it converts PAST the ceiling, overflows to a negative value, and is
	// rejected as "invalid timestamp value" before the recursion check this case
	// is actually about ever runs.
	//
	// Derive the literal from the ceiling in the local zone instead, so the
	// expected message is the same everywhere. A full day of margin absorbs the
	// widest real UTC offset and any DST rule extrapolated into 2262, and
	// formatting to second precision only ever rounds down.
	farFutureAsOf := time.Unix(0, math.MaxInt64).In(time.Local).
		Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
	for _, test := range []struct {
		name            string
		sql             string
		wantErr         string
		wantIfNotExists bool
		timestampValid  bool
		lowerCaseMode   *int64
		withoutTxn      bool
	}{
		{
			name:    "direct reference",
			sql:     "create or replace view v as select n_nationkey from v",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:    "indirect reference",
			sql:     "create or replace view v as select n_nationkey from v2",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:          "mixed case direct reference lctn2",
			sql:           "create or replace view V as select n_nationkey from v",
			wantErr:       "internal error: there is a recursive reference to the view V",
			lowerCaseMode: &lctn2,
		},
		{
			name:          "mixed case schema reference lctn2",
			sql:           "create or replace view TPCH.V as select n_nationkey from tpch.v",
			wantErr:       "internal error: there is a recursive reference to the view V",
			lowerCaseMode: &lctn2,
		},
		{
			name:          "mixed case indirect reference lctn2",
			sql:           "create or replace view V as select * from v2",
			wantErr:       "internal error: there is a recursive reference to the view V",
			lowerCaseMode: &lctn2,
		},
		{
			name:          "mixed case cross database reference lctn2",
			sql:           "create or replace view TPCH.V as select * from other.v3",
			wantErr:       "internal error: there is a recursive reference to the view V",
			lowerCaseMode: &lctn2,
		},
		{
			name:          "mixed case alter reference lctn2",
			sql:           "alter view V as select n_nationkey from v",
			wantErr:       "internal error: there is a recursive reference to the view V",
			lowerCaseMode: &lctn2,
		},
		{
			name:    "mixed case normalized reference lctn1",
			sql:     "create or replace view V as select n_nationkey from v",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:          "mixed case distinct reference lctn0",
			sql:           "create or replace view V as select n_nationkey from v",
			lowerCaseMode: &lctn0,
		},
		{
			name:          "persisted lctn0 nested view remains distinct under lctn2",
			sql:           "create or replace view v as select * from i",
			lowerCaseMode: &lctn2,
		},
		{
			name: "historical snapshot reference",
			sql:  "create or replace view v as select n_nationkey from v {snapshot = 'sp'}",
		},
		{
			name:           "historical timestamp reference",
			sql:            "create or replace view v as select n_nationkey from v {timestamp = '2020-01-01 00:00:00'}",
			timestampValid: true,
		},
		{
			name: "recent timestamp reference",
			sql:  fmt.Sprintf("create or replace view v as select n_nationkey from v {timestamp = '%s'}", recentTimestamp),
		},
		{
			name: "historical MO_TS reference",
			sql:  "create or replace view v as select n_nationkey from v {MO_TS = '1577836800000000000-0'}",
		},
		{
			name: "historical AS OF timestamp reference",
			sql:  "create or replace view v as select n_nationkey from v {as of timestamp '2020-01-01 00:00:00'}",
		},
		{
			name: "MO_TS ahead of wall clock but behind transaction HLC",
			sql: fmt.Sprintf(
				"create or replace view v as select n_nationkey from v {MO_TS = '%d-0'}",
				aheadOfWallClock.UnixNano(),
			),
		},
		{
			name: "AS OF timestamp ahead of wall clock but behind transaction HLC",
			sql: fmt.Sprintf(
				"create or replace view v as select n_nationkey from v {as of timestamp '%s'}",
				aheadOfWallClock.In(time.Local).Format("2006-01-02 15:04:05"),
			),
		},
		{
			name: "timestamp ahead of wall clock with catalog snapshot",
			sql: fmt.Sprintf(
				"create or replace view v as select n_nationkey from v {timestamp = '%s'}",
				aheadOfWallClock.UTC().Format("2006-01-02 15:04:05.999999999"),
			),
			timestampValid: true,
		},
		{
			name:    "future integer timestamp",
			sql:     "create or replace view v as select n_nationkey from v {timestamp = 9223372036854775807}",
			wantErr: "invalid argument invalid timestamp value, no corresponding snapshot , bad value 9223372036854775807",
		},
		{
			name:    "future MO_TS",
			sql:     "create or replace view v as select n_nationkey from v {MO_TS = '9223372036854775807-0'}",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:    "future integer MO_TS",
			sql:     "create or replace view v as select n_nationkey from v {MO_TS = 9223372036854775807}",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:    "future AS OF timestamp",
			sql:     "create or replace view v as select n_nationkey from v {as of timestamp '" + farFutureAsOf + "'}",
			wantErr: "internal error: there is a recursive reference to the view v",
		},
		{
			name:       "future MO_TS without transaction fails closed",
			sql:        "create or replace view v as select n_nationkey from v {MO_TS = '9223372036854775807-0'}",
			wantErr:    "internal error: there is a recursive reference to the view v",
			withoutTxn: true,
		},
		{
			name:    "past timestamp without snapshot",
			sql:     "create or replace view v as select n_nationkey from v {timestamp = '2020-01-01 00:00:00'}",
			wantErr: "invalid argument invalid timestamp value, no corresponding snapshot , bad value 2020-01-01 00:00:00",
		},
		{
			name:            "if not exists keeps body as no-op",
			sql:             "create or replace view if not exists v as select n_nationkey from v",
			wantIfNotExists: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lowerCaseMode := int64(1)
			if test.lowerCaseMode != nil {
				lowerCaseMode = *test.lowerCaseMode
			}
			mock := NewMockCompilerContext(false)
			proc := testutil.NewProc(nil)
			if !test.withoutTxn {
				proc.Base.TxnOperator = viewReplacementTxnOperator{snapshotTS: currentTxnSnapshot}
			}
			mock.GetProcessFunc = func() *process.Process { return proc }
			mock.dbs["other"] = true
			mock.tables["v"] = newViewDef(t, "create view v as select n_nationkey from nation", "tpch", nil)
			mock.tables["v2"] = newViewDef(t, "create view v2 as select * from v", "tpch", nil)
			mock.tables["v3"] = newViewDef(t, "create view v3 as select * from tpch.v", "other", nil)
			mock.tables["i"] = newViewDef(t, "create view i as select * from V", "tpch", &lctn0)
			mock.objects["v"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v"}
			mock.objects["v2"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v2"}
			mock.objects["v3"] = &plan.ObjectRef{SchemaName: "other", ObjName: "v3"}
			mock.objects["i"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "i"}
			ctx := &viewReplacementCompilerContext{
				rootSQLCompilerContext: &rootSQLCompilerContext{
					MockCompilerContext: mock,
					rootSQL:             test.sql,
				},
				historicalSnapshot: &Snapshot{
					TS: &timestamp.Timestamp{PhysicalTime: 1},
				},
				timestampValid:      test.timestampValid,
				lowerCaseTableNames: lowerCaseMode,
			}

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, test.sql, lowerCaseMode)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(ctx, stmt, false)
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				createView := p.GetDdl().GetCreateView()
				require.NotNil(t, createView)
				require.True(t, createView.GetReplace())
				require.Equal(t, test.wantIfNotExists, createView.GetIfNotExists())
			}
			require.False(t, ctx.building)
			require.Empty(t, ctx.database)
			require.Empty(t, ctx.view)
		})
	}
}

func TestBuildCreateTableCheckConstraints(t *testing.T) {
	build := func(sql string, prepare bool) (*plan.TableDef, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(NewMockCompilerContext(false), stmt, prepare)
		if err != nil {
			return nil, err
		}
		return p.GetDdl().GetCreateTable().GetTableDef(), nil
	}

	t.Run("table check binds after all columns", func(t *testing.T) {
		tableDef, err := build("create table t(a int, check (b > a), b int)", false)
		require.NoError(t, err)
		require.Len(t, tableDef.Checks, 1)
		require.Equal(t, "__mo_chk_1", tableDef.Checks[0].Name)
		require.Equal(t, int32(types.T_bool), tableDef.Checks[0].Check.Typ.Id)
	})

	t.Run("table check preserves explicit name", func(t *testing.T) {
		tableDef, err := build(
			"create table t(a int, constraint positive_a check (a > 0))",
			false,
		)
		require.NoError(t, err)
		require.Len(t, tableDef.Checks, 1)
		require.Equal(t, "positive_a", tableDef.Checks[0].Name)
	})

	t.Run("column check only references its column", func(t *testing.T) {
		_, err := build("create table t(a int, b int check (a > b))", false)
		require.ErrorContains(t, err, "column check constraint cannot refer to column")
	})

	t.Run("ctas explicit column preserves column check", func(t *testing.T) {
		tableDef, err := build(
			"create table t(a int constraint positive_a check (a > 0)) as select 1 as a",
			false,
		)
		require.NoError(t, err)
		require.Len(t, tableDef.Checks, 1)
		require.Equal(t, "positive_a", tableDef.Checks[0].Name)
		require.Equal(t, "`a` > 0", tableDef.Checks[0].OriginSql)
	})

	t.Run("check origin sql uses replay-safe string quoting", func(t *testing.T) {
		tableDef, err := build(
			"create table t(s varchar(10) check (s = 'ok'))",
			false,
		)
		require.NoError(t, err)
		require.Len(t, tableDef.Checks, 1)
		require.Equal(t, "`s` = 'ok'", tableDef.Checks[0].OriginSql)
	})

	t.Run("name const cast name remains invalid", func(t *testing.T) {
		_, err := build(
			"create table t(a int, "+
				"check (name_const(cast(0x61 as varchar), 1) = 1))",
			false,
		)
		require.ErrorContains(t, err, "invalid argument NAME_CONST")
	})

	t.Run("non boolean root is converted", func(t *testing.T) {
		tableDef, err := build("create table t(a int, check (a))", false)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_bool), tableDef.Checks[0].Check.Typ.Id)
		require.Equal(t, "cast", tableDef.Checks[0].Check.GetF().GetFunc().GetObjName())
	})

	t.Run("auto increment references are rejected", func(t *testing.T) {
		_, err := build("create table t(a int auto_increment primary key, check (a > 0))", false)
		require.ErrorContains(t, err, "cannot refer to auto-increment column")
	})

	t.Run("session dependent functions are rejected", func(t *testing.T) {
		_, err := build("create table t(a int, check (current_user_id() = a))", false)
		require.ErrorContains(t, err, "session-dependent function")
	})

	t.Run("not enforced is explicit and unsupported", func(t *testing.T) {
		_, err := build("create table t(a int check (a > 0) not enforced)", false)
		require.ErrorContains(t, err, "NOT ENFORCED CHECK constraints")
	})

	t.Run("external table column check is unsupported", func(t *testing.T) {
		_, err := build(
			"create external table t(a int check (a > 0)) "+
				"infile{'filepath'='/tmp/t.csv'}",
			false,
		)
		require.ErrorContains(t, err, "CHECK constraints on external tables")
	})

	t.Run("external table table check is unsupported", func(t *testing.T) {
		_, err := build(
			"create external table t(a int, check (a > 0)) "+
				"infile{'filepath'='/tmp/t.csv'}",
			false,
		)
		require.ErrorContains(t, err, "CHECK constraints on external tables")
	})

	t.Run("invalid function and marker do not panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			_, err := build("create table t(a int, check (no_such_func(a) > 0))", false)
			require.Error(t, err)
		})
		require.NotPanics(t, func() {
			_, err := build("create table t(a int, check (? > 0))", true)
			require.Error(t, err)
		})
	})

	t.Run("mixed version cluster rejects check ddl", func(t *testing.T) {
		ctx := NewMockCompilerContext(false)
		proc := ctx.GetProcess()
		rt := moruntime.ServiceRuntime(proc.GetService())
		old, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion6)
		defer func() {
			if ok {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		stmt, err := parsers.ParseOne(
			t.Context(),
			dialect.MYSQL,
			"create table t(a int, check (a > 0))",
			1,
		)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(ctx, stmt, false)
		require.ErrorContains(t, err, "protocol version 7")
	})
}

func TestBuildCreateTableAutoIncrementOffset(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		wantOffset uint64
	}{
		{name: "session offset", sql: "create table t(id int auto_increment)", wantOffset: 9},
		{name: "zero keeps session offset", sql: "create table t(id int auto_increment) auto_increment = 0", wantOffset: 9},
		{name: "nonzero overrides session offset", sql: "create table t(id int auto_increment) auto_increment = 100", wantOffset: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := &autoIncrementOffsetCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				offset:              10,
			}
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			require.Equal(t, tc.wantOffset, p.GetDdl().GetCreateTable().GetTableDef().GetAutoIncrOffset())
		})
	}
}

func tableDefCreateSQL(tableDef *plan.TableDef) string {
	for _, def := range tableDef.GetDefs() {
		for _, property := range def.GetProperties().GetProperties() {
			if property.GetKey() == catalog.SystemRelAttr_CreateSQL {
				return property.GetValue()
			}
		}
	}
	return ""
}

func TestGenViewTableDefCapturesRootSQLOnce(t *testing.T) {
	const rootSQL = "create view v as select 1"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, 1, ctx.calls)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Equal(t, rootSQL, viewData.Stmt)

	var createSQL string
	for _, def := range tableDef.GetDefs() {
		for _, property := range def.GetProperties().GetProperties() {
			if property.GetKey() == catalog.SystemRelAttr_CreateSQL {
				createSQL = property.GetValue()
			}
		}
	}
	require.Equal(t, rootSQL, createSQL)
}

func TestGenViewTableDefPersistsExpandedStarSelectList(t *testing.T) {
	const rootSQL = "create view v_star as select * from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)
	require.Len(t, tableDef.GetCols(), 4)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.NotContains(t, viewData.Stmt, "*")
	require.Contains(t, viewData.Stmt, "`nation`.`n_nationkey`")
	require.Contains(t, viewData.Stmt, "`nation`.`n_name`")
	require.Contains(t, viewData.Stmt, "`nation`.`n_regionkey`")
	require.Contains(t, viewData.Stmt, "`nation`.`n_comment`")

	createSQL := tableDefCreateSQL(tableDef)
	require.Equal(t, viewData.Stmt, createSQL)

	ctx.tables["v_star"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_star"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_star"}
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &plan.ColDef{
		Name:       "n_extra",
		OriginName: "n_extra",
		Typ:        plan.Type{Id: int32(types.T_int32)},
		Default:    &plan.Default{NullAbility: true},
	})

	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_star", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}, selectPlan.GetQuery().GetHeadings())

	missingStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select n_extra from v_star", 1)
	require.NoError(t, err)
	defer missingStmt.Free()
	_, err = BuildPlan(ctx, missingStmt, false)
	require.ErrorContains(t, err, "column n_extra does not exist")
}

func TestGenViewTableDefExpandedStarFromDerivedAggregateCanRebind(t *testing.T) {
	const rootSQL = "create view v_star_agg as select * from (select id,min(ti) from (select * from t1) t1 group by id) sub"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.tables["t1"] = &plan.TableDef{
		Name:      "t1",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{
			{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "ti", OriginName: "ti", Typ: plan.Type{Id: int32(types.T_uint8)}, Default: &plan.Default{NullAbility: true}},
		},
	}
	ctx.objects["t1"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "t1"}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)
	require.Equal(t, []string{"id", "min(ti)"}, []string{tableDef.GetCols()[0].Name, tableDef.GetCols()[1].Name})

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Contains(t, viewData.Stmt, "`sub`.`min(ti)`")
	require.Contains(t, viewData.Stmt, "min(`t1`.`ti`)")
	require.Contains(t, viewData.Stmt, "as `min(ti)`")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	stableStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewData.Stmt, 1)
	require.NoError(t, err)
	defer stableStmt.Free()
	_, err = BuildPlan(ctx, stableStmt, false)
	require.NoError(t, err)

	ctx.tables["v_star_agg"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_star_agg"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_star_agg"}

	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_star_agg", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "min(ti)"}, selectPlan.GetQuery().GetHeadings())
}

func TestGenViewTableDefDoesNotRewriteCountStar(t *testing.T) {
	const rootSQL = "create view v_count as select count(*) from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Equal(t, rootSQL, viewData.Stmt)
	require.Equal(t, rootSQL, tableDefCreateSQL(tableDef))
}

func TestGenViewTableDefFreezesSampleStar(t *testing.T) {
	const rootSQL = "create view v_sample as select sample(*, 100 percent) from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Contains(t, viewData.Stmt, "sample(`nation`.`n_nationkey`, `nation`.`n_name`, `nation`.`n_regionkey`, `nation`.`n_comment`, 100.0 percent)")
	require.NotContains(t, viewData.Stmt, "sample(*")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	ctx.tables["v_sample"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_sample"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_sample"}
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &plan.ColDef{
		Name:       "n_extra",
		OriginName: "n_extra",
		Typ:        plan.Type{Id: int32(types.T_int32)},
		Default:    &plan.Default{NullAbility: true},
	})
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_sample", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}, selectPlan.GetQuery().GetHeadings())
}

func TestGenViewTableDefExpandsOuterStarWithNestedSample(t *testing.T) {
	const rootSQL = "create view v_outer_sample as select * from nation where exists (select sample(*, 100 percent) from region)"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Contains(t, viewData.Stmt, "sample(`region`.`r_regionkey`, `region`.`r_name`, `region`.`r_comment`, 100.0 percent)")
	require.NotContains(t, viewData.Stmt, "sample(*")
	require.Contains(t, viewData.Stmt, "`nation`.`n_nationkey`")
	require.NotContains(t, viewData.Stmt, "`nation`.*")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	ctx.tables["v_outer_sample"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_outer_sample"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_outer_sample"}
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &plan.ColDef{
		Name:       "n_extra",
		OriginName: "n_extra",
		Typ:        plan.Type{Id: int32(types.T_int32)},
		Default:    &plan.Default{NullAbility: true},
	})
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_outer_sample", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}, selectPlan.GetQuery().GetHeadings())
}

func TestGenViewTableDefRewritesSubqueryInsideSampleColumns(t *testing.T) {
	const rootSQL = "create view v_sample_subquery as select sample((select * from one_col union all select 1), 1 rows) from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	addOneColViewStarTestTable(ctx.MockCompilerContext)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.NotContains(t, viewData.Stmt, "select * from `one_col`")
	require.Contains(t, viewData.Stmt, "select `one_col`.`id` as `id` from `one_col`")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	ctx.tables["v_sample_subquery"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_sample_subquery"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_sample_subquery"}
	appendOneColExtraColumn(ctx.MockCompilerContext)
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_sample_subquery", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	_, err = BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
}

func TestGenViewTableDefExpandsMixedStarAndSample(t *testing.T) {
	const rootSQL = "create view v_mixed_sample as select *, sample(*, 100 percent) from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.Contains(t, viewData.Stmt, "sample(`nation`.`n_nationkey`, `nation`.`n_name`, `nation`.`n_regionkey`, `nation`.`n_comment`, 100.0 percent)")
	require.NotContains(t, viewData.Stmt, "sample(*")
	require.NotContains(t, viewData.Stmt, "`nation`.*")
	require.Contains(t, viewData.Stmt, "`nation`.`n_nationkey`")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	stableStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewData.Stmt, 1)
	require.NoError(t, err)
	defer stableStmt.Free()
	_, err = BuildPlan(ctx, stableStmt, false)
	require.NoError(t, err)
}

func TestGenViewTableDefExpandsGroupingSetStars(t *testing.T) {
	tests := []struct {
		name     string
		viewName string
		stmt     string
	}{
		{name: "rollup", viewName: "v_rollup", stmt: "create view v_rollup as select * from one_col group by id with rollup"},
		{name: "cube", viewName: "v_cube", stmt: "create view v_cube as select * from one_col group by cube(id)"},
		{name: "grouping sets", viewName: "v_grouping_sets", stmt: "create view v_grouping_sets as select * from one_col group by grouping sets ((id), ())"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &rootSQLCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				rootSQL:             tt.stmt,
			}
			addOneColViewStarTestTable(ctx.MockCompilerContext)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tt.stmt, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			tableDef := p.GetDdl().GetCreateView().GetTableDef()
			require.NotNil(t, tableDef)

			var viewData ViewData
			require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
			require.NotContains(t, viewData.Stmt, "*")
			require.Contains(t, viewData.Stmt, "`one_col`.`id`")
			require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

			ctx.tables[tt.viewName] = DeepCopyTableDef(tableDef, true)
			ctx.objects[tt.viewName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: tt.viewName}
			appendOneColExtraColumn(ctx.MockCompilerContext)
			selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from "+tt.viewName, 1)
			require.NoError(t, err)
			defer selectStmt.Free()
			_, err = BuildPlan(ctx, selectStmt, false)
			require.NoError(t, err)
		})
	}
}

func TestGenViewTableDefPersistsExpandedUnionStars(t *testing.T) {
	const rootSQL = "create view v_union as select * from nation union all select * from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.NotContains(t, viewData.Stmt, "*")
	require.Equal(t, 2, strings.Count(viewData.Stmt, "`nation`.`n_nationkey`"))
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))
}

func TestNormalSelectDoesNotCaptureExpandedStarList(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from nation", 1)
	require.NoError(t, err)
	defer stmt.Free()

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, true)
	bindCtx := NewBindContext(builder, nil)
	_, err = builder.bindSelect(stmt.(*tree.Select), bindCtx, true)
	require.NoError(t, err)
	require.Nil(t, bindCtx.expandedSelectLists)
}

func TestStableViewStarHelpersCoverASTShapes(t *testing.T) {
	starExpr := tree.SelectExpr{Expr: tree.UnqualifiedStar{}}
	qualifiedStar := tree.SelectExpr{Expr: tree.NewUnresolvedNameWithStar(tree.NewCStr("t", 1))}
	columnExpr := tree.SelectExpr{Expr: tree.NewUnresolvedColName("c")}
	sampleStar, err := tree.NewSamplePercentFuncExpression1(50, true, nil)
	require.NoError(t, err)
	sampleColumn, err := tree.NewSamplePercentFuncExpression1(50, false, tree.Exprs{tree.NewUnresolvedColName("c")})
	require.NoError(t, err)

	require.False(t, viewSelectHasStar(nil))
	require.True(t, selectExprHasStar(starExpr))
	require.True(t, selectExprHasStar(qualifiedStar))
	require.False(t, selectExprHasStar(columnExpr))
	require.True(t, selectExprHasStar(tree.SelectExpr{Expr: sampleStar}))
	require.False(t, selectExprHasStar(tree.SelectExpr{Expr: sampleColumn}))

	starClause := &tree.SelectClause{Exprs: tree.SelectExprs{starExpr}}
	columnClause := &tree.SelectClause{Exprs: tree.SelectExprs{columnExpr}}
	wrappedStar := &tree.Select{Select: starClause}
	parenStar := &tree.ParenSelect{Select: wrappedStar}
	union := &tree.UnionClause{Left: starClause, Right: columnClause}
	require.True(t, selectStatementHasStar(starClause))
	require.True(t, selectStatementHasStar(wrappedStar))
	require.True(t, selectStatementHasStar(parenStar))
	require.True(t, selectStatementHasStar(union))
	require.False(t, selectStatementHasStar(nil))
	require.False(t, selectStatementHasStar(columnClause))

	nestedStarClause := &tree.SelectClause{
		Exprs: tree.SelectExprs{starExpr},
		From:  &tree.From{},
	}
	nestedWindowClause := &tree.SelectClause{
		Exprs: tree.SelectExprs{columnExpr},
		From:  &tree.From{},
		Windows: tree.WindowDefinitions{
			&tree.WindowDefinition{
				Name: tree.NewCStr("w", 1),
				Spec: &tree.WindowSpec{PartitionBy: tree.Exprs{
					&tree.Subquery{Select: &tree.Select{Select: nestedStarClause}},
				}},
			},
		},
	}
	require.True(t, selectClauseHasStar(nestedWindowClause))
	stableWindowStmt, rewritten := viewSelectStatementWithExpandedStars(
		nestedWindowClause,
		map[*tree.SelectClause]tree.SelectExprs{
			nestedStarClause: {{Expr: tree.NewUnresolvedColName("stable_col")}},
		},
	)
	require.True(t, rewritten)
	require.NotContains(t, tree.String(stableWindowStmt, dialect.MYSQL), "select *")
	require.Contains(t, tree.String(stableWindowStmt, dialect.MYSQL), "stable_col")

}

func TestStableViewStarHelpersRewriteNestedTableExpressions(t *testing.T) {
	makeStarSelect := func() (*tree.Select, *tree.SelectClause) {
		clause := &tree.SelectClause{
			Exprs: tree.SelectExprs{{Expr: tree.UnqualifiedStar{}}},
			From:  &tree.From{},
		}
		return &tree.Select{Select: clause}, clause
	}
	replacement := func() tree.SelectExprs {
		return tree.SelectExprs{{Expr: tree.NewUnresolvedColName("stable_col")}}
	}

	selectTable, selectClause := makeStarSelect()
	subquerySelect, subqueryClause := makeStarSelect()
	aliasedSelect, aliasedClause := makeStarSelect()
	parenSelect, parenClause := makeStarSelect()
	joinLeft, joinLeftClause := makeStarSelect()
	joinRight, joinRightClause := makeStarSelect()
	applyLeft, applyLeftClause := makeStarSelect()
	applyRight, applyRightClause := makeStarSelect()
	sourceSelect, sourceClause := makeStarSelect()

	expanded := map[*tree.SelectClause]tree.SelectExprs{
		selectClause:     replacement(),
		subqueryClause:   replacement(),
		aliasedClause:    replacement(),
		parenClause:      replacement(),
		joinLeftClause:   replacement(),
		joinRightClause:  replacement(),
		applyLeftClause:  replacement(),
		applyRightClause: replacement(),
		sourceClause:     replacement(),
	}
	tables := tree.TableExprs{
		selectTable,
		&tree.Subquery{Select: subquerySelect},
		&tree.AliasedTableExpr{Expr: aliasedSelect},
		&tree.ParenTableExpr{Expr: parenSelect},
		tree.NewJoinTableExpr(tree.JOIN_TYPE_INNER, joinLeft, joinRight, nil),
		&tree.ApplyTableExpr{Left: applyLeft, Right: applyRight},
		tree.NewStatementSource(sourceSelect),
		&tree.TableName{},
	}
	stableTables, rewritten := viewTableExprsWithExpandedStars(tables, expanded)
	require.True(t, rewritten)
	require.Len(t, stableTables, len(tables))
	require.NotSame(t, tables[0], stableTables[0])

	stableFrom, rewritten := viewFromWithExpandedStars(&tree.From{Tables: tables}, expanded)
	require.True(t, rewritten)
	require.Len(t, stableFrom.Tables, len(tables))
	_, rewritten = viewFromWithExpandedStars(nil, expanded)
	require.False(t, rewritten)
	_, rewritten = viewTableExprsWithExpandedStars(nil, expanded)
	require.False(t, rewritten)

	stable, rewritten := viewSelectWithExpandedStars(&tree.Select{Select: &tree.SelectClause{From: &tree.From{Tables: tables}}}, expanded)
	require.True(t, rewritten)
	require.NotNil(t, stable)
	_, rewritten = viewSelectWithExpandedStars(nil, expanded)
	require.False(t, rewritten)

	// Exercise the statement wrappers and their defensive failure paths.
	wrapped := &tree.Select{Select: &tree.SelectClause{From: &tree.From{Tables: tree.TableExprs{&tree.ParenTableExpr{Expr: selectTable}}}}}
	_, rewritten = viewSelectStatementWithExpandedStars(wrapped, expanded)
	require.True(t, rewritten)
	paren := &tree.ParenSelect{Select: selectTable}
	_, rewritten = viewSelectStatementWithExpandedStars(paren, expanded)
	require.True(t, rewritten)
	_, rewritten = viewSelectStatementWithExpandedStars(&tree.ParenSelect{Select: &tree.Select{}}, expanded)
	require.False(t, rewritten)
	_, rewritten = viewSelectStatementWithExpandedStars(&tree.Select{Select: nil}, expanded)
	require.False(t, rewritten)
	_, rewritten = viewSelectStatementWithExpandedStars(&tree.UnionClause{Left: nil, Right: selectTable}, expanded)
	require.False(t, rewritten)
}

func TestGenViewTableDefPersistsExpandedCTEStars(t *testing.T) {
	const rootSQL = "create view v_cte as with recursive c(n_nationkey,n_name,n_regionkey,n_comment) as (select * from nation union all select n_nationkey,n_name,n_regionkey,n_comment from c where false) select * from c"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNil(t, tableDef)
	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.NotContains(t, viewData.Stmt, "*")
	require.Contains(t, viewData.Stmt, "`nation`.`n_nationkey`")
	require.Contains(t, viewData.Stmt, "with recursive `c`")

	stableStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewData.Stmt, 1)
	require.NoError(t, err)
	defer stableStmt.Free()
	_, err = BuildPlan(ctx, stableStmt, false)
	require.NoError(t, err)

	ctx.tables["v_cte"] = DeepCopyTableDef(tableDef, true)
	ctx.objects["v_cte"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_cte"}
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &plan.ColDef{
		Name:       "n_extra",
		OriginName: "n_extra",
		Typ:        plan.Type{Id: int32(types.T_int32)},
		Default:    &plan.Default{NullAbility: true},
	})

	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_cte", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}, selectPlan.GetQuery().GetHeadings())
}

func addOneColViewStarTestTable(ctx *MockCompilerContext) {
	ctx.tables["one_col"] = &plan.TableDef{
		Name:      "one_col",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{
			{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
		},
	}
	ctx.objects["one_col"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "one_col"}
}

func appendOneColExtraColumn(ctx *MockCompilerContext) {
	ctx.tables["one_col"].Cols = append(ctx.tables["one_col"].Cols, &plan.ColDef{
		Name:       "extra",
		OriginName: "extra",
		Typ:        plan.Type{Id: int32(types.T_int32)},
		Default:    &plan.Default{NullAbility: true},
	})
}

func TestGenViewTableDefPersistsExpandedExpressionSubqueryStars(t *testing.T) {
	tests := []struct {
		name      string
		viewName  string
		rootSQL   string
		headings  []string
		stableCol string
	}{
		{
			name:      "scalar subquery in select list",
			viewName:  "v_nested_scalar",
			rootSQL:   "create view v_nested_scalar as select (select * from one_col) as x",
			headings:  []string{"x"},
			stableCol: "`one_col`.`id`",
		},
		{
			name:      "in subquery in where",
			viewName:  "v_nested_in",
			rootSQL:   "create view v_nested_in as select n_nationkey from nation where n_nationkey in (select * from one_col)",
			headings:  []string{"n_nationkey"},
			stableCol: "`one_col`.`id`",
		},
		{
			name:      "in subquery in join on",
			viewName:  "v_join_star",
			rootSQL:   "create view v_join_star as select n.n_nationkey from nation n join region r on n.n_regionkey = r.r_regionkey and n.n_nationkey in (select * from one_col)",
			headings:  []string{"n_nationkey"},
			stableCol: "`one_col`.`id`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &rootSQLCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				rootSQL:             tt.rootSQL,
			}
			addOneColViewStarTestTable(ctx.MockCompilerContext)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tt.rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			tableDef := p.GetDdl().GetCreateView().GetTableDef()
			require.NotNil(t, tableDef)

			var viewData ViewData
			require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
			require.NotContains(t, viewData.Stmt, "*")
			require.Contains(t, viewData.Stmt, tt.stableCol)
			require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

			stableStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewData.Stmt, 1)
			require.NoError(t, err)
			defer stableStmt.Free()
			_, err = BuildPlan(ctx, stableStmt, false)
			require.NoError(t, err)

			ctx.tables[tt.viewName] = DeepCopyTableDef(tableDef, true)
			ctx.objects[tt.viewName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: tt.viewName}
			appendOneColExtraColumn(ctx.MockCompilerContext)

			selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from "+tt.viewName, 1)
			require.NoError(t, err)
			defer selectStmt.Free()
			selectPlan, err := BuildPlan(ctx, selectStmt, false)
			require.NoError(t, err)
			require.Equal(t, tt.headings, selectPlan.GetQuery().GetHeadings())
		})
	}
}

func TestBuildAlterViewPersistsExpandedJoinOnSubqueryStars(t *testing.T) {
	const alterSQL = "alter view v_join_star as select n.n_nationkey from nation n join region r on n.n_regionkey = r.r_regionkey and n.n_nationkey in (select * from one_col)"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             alterSQL,
	}
	addOneColViewStarTestTable(ctx.MockCompilerContext)
	ctx.tables["v_join_star"] = &plan.TableDef{
		Name:      "v_join_star",
		TableType: catalog.SystemViewRel,
		Cols: []*plan.ColDef{
			{Name: "n_nationkey", OriginName: "n_nationkey", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
		},
		ViewSql: &plan.ViewDef{View: `{"Stmt":"create view v_join_star as select n_nationkey from nation","DefaultDatabase":"tpch","SecurityType":"DEFINER"}`},
	}
	ctx.objects["v_join_star"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_join_star"}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, alterSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	tableDef := p.GetDdl().GetAlterView().GetTableDef()
	require.NotNil(t, tableDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(tableDef.GetViewSql().GetView()), &viewData))
	require.NotContains(t, viewData.Stmt, "*")
	require.Contains(t, viewData.Stmt, "`one_col`.`id`")
	require.Equal(t, viewData.Stmt, tableDefCreateSQL(tableDef))

	stableStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewData.Stmt, 1)
	require.NoError(t, err)
	defer stableStmt.Free()
	_, err = BuildPlan(ctx, stableStmt, false)
	require.NoError(t, err)

	ctx.tables["v_join_star"] = DeepCopyTableDef(tableDef, true)
	appendOneColExtraColumn(ctx.MockCompilerContext)
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select * from v_join_star", 1)
	require.NoError(t, err)
	defer selectStmt.Free()
	selectPlan, err := BuildPlan(ctx, selectStmt, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey"}, selectPlan.GetQuery().GetHeadings())
}

func TestStableViewSQLWithExpandedStarsRewritesAlterExpressionSubquery(t *testing.T) {
	const alterSQL = "alter view v_nested_scalar as select (select * from one_col) as x"
	parsed, err := parsers.ParseOne(context.Background(), dialect.MYSQL, alterSQL, 1)
	require.NoError(t, err)
	defer parsed.Free()
	alterView := parsed.(*tree.AlterView)
	rootClause := alterView.AsSource.Select.(*tree.SelectClause)
	subquery := rootClause.Exprs[0].Expr.(*tree.Subquery)
	subqueryClause := subquery.Select.(*tree.ParenSelect).Select.Select.(*tree.SelectClause)
	expanded := map[*tree.SelectClause]tree.SelectExprs{
		subqueryClause: {{Expr: tree.NewUnresolvedColName("id")}},
	}

	got, rewritten := stableViewSQLWithExpandedStars(NewMockCompilerContext(false), alterView.AsSource, alterSQL, expanded)
	require.True(t, rewritten)
	require.Contains(t, got, "create view")
	require.NotContains(t, got, "*")
	require.Contains(t, got, "`id`")
}

func TestStableViewStarHelpersRewriteCTEUnionAndRecursiveBranches(t *testing.T) {
	makeStarSelect := func() (*tree.Select, *tree.SelectClause) {
		clause := &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: tree.UnqualifiedStar{}}}, From: &tree.From{}}
		return &tree.Select{Select: clause}, clause
	}
	left, leftClause := makeStarSelect()
	right, rightClause := makeStarSelect()
	union := &tree.UnionClause{Left: left, Right: right}
	with := &tree.With{
		IsRecursive: true,
		CTEs: []*tree.CTE{
			{Name: &tree.AliasClause{Alias: "c"}, Stmt: union},
		},
	}
	root := &tree.Select{With: with, Select: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: tree.NewUnresolvedNameWithStar(tree.NewCStr("c", 1))}}, From: &tree.From{}}}
	expanded := map[*tree.SelectClause]tree.SelectExprs{
		leftClause:  {{Expr: tree.NewUnresolvedColName("left_col")}},
		rightClause: {{Expr: tree.NewUnresolvedColName("right_col")}},
	}
	require.True(t, viewSelectHasStar(root))
	stable, rewritten := viewSelectWithExpandedStars(root, expanded)
	require.True(t, rewritten)
	require.NotNil(t, stable)
	stableWith := stable.With
	require.NotNil(t, stableWith)
	require.Len(t, stableWith.CTEs, 1)
	stableUnion, ok := stableWith.CTEs[0].Stmt.(*tree.UnionClause)
	require.True(t, ok)
	stableLeft := stableUnion.Left.(*tree.Select).Select.(*tree.SelectClause)
	stableRight := stableUnion.Right.(*tree.Select).Select.(*tree.SelectClause)
	require.Equal(t, "left_col", stableLeft.Exprs[0].Expr.(*tree.UnresolvedName).ColName())
	require.Equal(t, "right_col", stableRight.Exprs[0].Expr.(*tree.UnresolvedName).ColName())
}

func TestStableViewSQLWithExpandedStarsRejectsUnsupportedInputs(t *testing.T) {
	const viewSQL = "create view v_star as select * from nation"
	parsed, err := parsers.ParseOne(context.Background(), dialect.MYSQL, viewSQL, 1)
	require.NoError(t, err)
	defer parsed.Free()
	createView := parsed.(*tree.CreateView)
	stmt := createView.AsSource
	clause := stmt.Select.(*tree.SelectClause)
	expanded := map[*tree.SelectClause]tree.SelectExprs{
		clause: {{Expr: tree.NewUnresolvedColName("stable_col")}},
	}
	ctx := NewMockCompilerContext(false)

	got, rewritten := stableViewSQLWithExpandedStars(ctx, stmt, "", expanded)
	require.Equal(t, "", got)
	require.False(t, rewritten)
	got, rewritten = stableViewSQLWithExpandedStars(ctx, stmt, viewSQL, nil)
	require.Equal(t, viewSQL, got)
	require.False(t, rewritten)
	noStar := &tree.Select{Select: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: tree.NewUnresolvedColName("c")}}, From: &tree.From{}}}
	got, rewritten = stableViewSQLWithExpandedStars(ctx, noStar, viewSQL, expanded)
	require.Equal(t, viewSQL, got)
	require.False(t, rewritten)

	sampleStar, err := tree.NewSamplePercentFuncExpression1(50, true, nil)
	require.NoError(t, err)
	sample := &tree.Select{Select: &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: sampleStar}}, From: &tree.From{}}}
	got, rewritten = stableViewSQLWithExpandedStars(ctx, sample, viewSQL, map[*tree.SelectClause]tree.SelectExprs{
		sample.Select.(*tree.SelectClause): expanded[clause],
	})
	require.Equal(t, viewSQL, got)
	require.False(t, rewritten)

	wrongClause := &tree.SelectClause{Exprs: tree.SelectExprs{{Expr: tree.UnqualifiedStar{}}}, From: &tree.From{}}
	got, rewritten = stableViewSQLWithExpandedStars(ctx, &tree.Select{Select: wrongClause}, viewSQL, expanded)
	require.Equal(t, viewSQL, got)
	require.False(t, rewritten)
	got, rewritten = stableViewSQLWithExpandedStars(ctx, stmt, "not valid sql", expanded)
	require.Equal(t, "not valid sql", got)
	require.False(t, rewritten)
	got, rewritten = stableViewSQLWithExpandedStars(ctx, stmt, viewSQL+";"+viewSQL, expanded)
	require.Equal(t, viewSQL+";"+viewSQL, got)
	require.False(t, rewritten)
	got, rewritten = stableViewSQLWithExpandedStars(ctx, stmt, "select * from nation", expanded)
	require.Equal(t, "select * from nation", got)
	require.False(t, rewritten)

	alterSQL := "alter view v_star as select * from nation"
	alterStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, alterSQL, 1)
	require.NoError(t, err)
	defer alterStmt.Free()
	alterView := alterStmt.(*tree.AlterView)
	alterSource := alterView.AsSource
	alterClause := alterSource.Select.(*tree.SelectClause)
	got, rewritten = stableViewSQLWithExpandedStars(ctx, alterSource, alterSQL, map[*tree.SelectClause]tree.SelectExprs{
		alterClause: expanded[clause],
	})
	require.True(t, rewritten)
	require.Contains(t, got, "create view")
}

func TestBuildCreateViewExplicitColumnList(t *testing.T) {
	t.Run("applies explicit names", func(t *testing.T) {
		const rootSQL = "create view v (`alias#one`, alias_two) as select 1, 2"
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
			rootSQL:             rootSQL,
		}
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()

		p, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		cols := p.GetDdl().GetCreateView().GetTableDef().GetCols()
		require.Len(t, cols, 2)
		require.Equal(t, "alias#one", cols[0].GetName())
		require.Equal(t, "alias#one", cols[0].GetOriginName())
		require.Equal(t, "alias_two", cols[1].GetName())
		require.Equal(t, "alias_two", cols[1].GetOriginName())
	})

	t.Run("rejects cardinality mismatch", func(t *testing.T) {
		const rootSQL = "create view v (only_one) as select 1, 2"
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
			rootSQL:             rootSQL,
		}
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()

		_, err = BuildPlan(ctx, stmt, false)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrViewWrongList))
		require.Equal(t, uint16(moerr.ER_VIEW_WRONG_LIST), err.(*moerr.Error).MySQLCode())
	})
}

func TestBuildCreateViewConsumesOutputColumnDefaultProvenance(t *testing.T) {
	tests := []struct {
		name         string
		selectSQL    string
		viewColumns  string
		wantDefault  bool
		wantNullable bool
	}{
		{name: "direct", selectSQL: "select n_nationkey from nation", wantDefault: true},
		{name: "alias", selectSQL: "select n_nationkey as qty from nation", wantDefault: true},
		{name: "explicit view column", selectSQL: "select n_nationkey from nation", viewColumns: "(qty)", wantDefault: true},
		{name: "derived table", selectSQL: "select qty from (select n_nationkey as qty from nation) d", wantDefault: true},
		{name: "non recursive cte", selectSQL: "with d as (select n_nationkey as qty from nation) select qty from d", wantDefault: true},
		{name: "constant", selectSQL: "select 7"},
		{name: "function", selectSQL: "select abs(n_nationkey) from nation"},
		{name: "arithmetic", selectSQL: "select n_nationkey + 0 from nation"},
		{name: "no unique provenance", selectSQL: "select coalesce(n_nationkey, 0) from nation"},
		{name: "aggregate", selectSQL: "select max(n_nationkey) from nation", wantNullable: true},
		{name: "union", selectSQL: "select n_nationkey from nation union select n_nationkey from nation"},
		{name: "union all", selectSQL: "select n_nationkey from nation union all select n_nationkey from nation"},
		{name: "recursive cte", selectSQL: "with recursive d(qty) as (select n_nationkey from nation union all select qty from d where false) select qty from d"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rootSQL := "create view v " + test.viewColumns + " as " + test.selectSQL
			ctx := &rootSQLCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				rootSQL:             rootSQL,
			}
			sourceCol := ctx.tables["nation"].Cols[0]
			sourceCol.Typ.NotNullable = true
			sourceCol.Default = &plan.Default{
				NullAbility:  false,
				Expr:         makePlan2Int32ConstExprWithType(7),
				OriginString: "7",
			}

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			viewPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			cols := viewPlan.GetDdl().GetCreateView().GetTableDef().GetCols()
			require.Len(t, cols, 1)
			def := cols[0].GetDefault()
			require.NotNil(t, def)
			require.Equal(t, test.wantNullable, def.GetNullAbility())
			if !test.wantDefault {
				require.Empty(t, def.GetOriginString())
				require.Nil(t, def.GetExpr())
				return
			}
			require.Equal(t, "7", def.GetOriginString())
			require.NotNil(t, def.GetExpr())
			require.Equal(t, int32(7), def.GetExpr().GetLit().GetI32Val())
			require.NotSame(t, sourceCol.Default.Expr, def.Expr)
		})
	}
}

func TestBuildCreateViewDefaultProvenanceAcrossBoundaries(t *testing.T) {
	newDefault := func(value int32) *plan.Default {
		return &plan.Default{
			Expr:         makePlan2Int32ConstExprWithType(value),
			OriginString: strconv.FormatInt(int64(value), 10),
		}
	}
	buildView := func(t *testing.T, ctx *MockCompilerContext, sql string) *plan.TableDef {
		t.Helper()
		rootCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(rootCtx, stmt, false)
		require.NoError(t, err)
		return p.GetDdl().GetCreateView().GetTableDef()
	}

	t.Run("view of view", func(t *testing.T) {
		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Cols[0].Typ.NotNullable = true
		ctx.tables["nation"].Cols[0].Default = newDefault(7)
		v1 := DeepCopyTableDef(buildView(t, ctx,
			"create view v1 as select n_nationkey as qty from nation"), true)
		v1.Name = "v1"
		v1.DbName = "tpch"
		v1.TableType = catalog.SystemViewRel
		ctx.tables[v1.Name] = v1
		ctx.objects[v1.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: v1.Name}

		v2 := buildView(t, ctx, "create view v2 as select qty as amount from v1")
		require.Len(t, v2.GetCols(), 1)
		require.Equal(t, "7", v2.GetCols()[0].GetDefault().GetOriginString())
		require.Equal(t, int32(7), v2.GetCols()[0].GetDefault().GetExpr().GetLit().GetI32Val())
	})

	t.Run("create or replace", func(t *testing.T) {
		ctx := NewMockCompilerContext(false)
		ctx.tables["nation"].Cols[0].Typ.NotNullable = true
		ctx.tables["nation"].Cols[0].Default = newDefault(7)
		const rootSQL = "create or replace view v_replace as select n_nationkey as qty from nation"
		rootCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: rootSQL}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(rootCtx, stmt, false)
		require.NoError(t, err)
		require.True(t, p.GetDdl().GetCreateView().GetReplace())
		require.Equal(t, "7", p.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetDefault().GetOriginString())
	})

	t.Run("multi table exact bound source", func(t *testing.T) {
		ctx := NewMockCompilerContext(false)
		left := ctx.tables["nation"].Cols[0]
		left.Typ.NotNullable = true
		left.Default = newDefault(7)
		rightTable := DeepCopyTableDef(ctx.tables["nation"], true)
		rightTable.Name = "nation2"
		rightTable.OriginalName = "nation2"
		rightTable.TblId++
		rightTable.Cols[0].Default = newDefault(9)
		ctx.tables[rightTable.Name] = rightTable
		ctx.objects[rightTable.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: rightTable.Name}

		viewDef := buildView(t, ctx, "create view v_join as "+
			"select l.n_nationkey as left_qty, r.n_nationkey as right_qty "+
			"from nation l join nation2 r on l.n_nationkey = r.n_nationkey")
		require.Len(t, viewDef.GetCols(), 2)
		require.Equal(t, "7", viewDef.GetCols()[0].GetDefault().GetOriginString())
		require.Equal(t, "9", viewDef.GetCols()[1].GetDefault().GetOriginString())

		leftJoinDef := buildView(t, ctx, "create view v_left_join as "+
			"select r.n_nationkey as right_qty from nation l left join nation2 r "+
			"on l.n_nationkey = r.n_nationkey")
		require.Len(t, leftJoinDef.GetCols(), 1)
		require.True(t, leftJoinDef.GetCols()[0].GetDefault().GetNullAbility())
		require.Equal(t, "9", leftJoinDef.GetCols()[0].GetDefault().GetOriginString())
		leftJoinDef.Name = "v_left_join"
		leftJoinDef.DbName = "tpch"
		leftJoinDef.TableType = catalog.SystemViewRel
		ctx.tables[leftJoinDef.Name] = leftJoinDef
		ctx.objects[leftJoinDef.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: leftJoinDef.Name}
		ctasStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table copied_left_join as select * from v_left_join", 1)
		require.NoError(t, err)
		defer ctasStmt.Free()
		ctasPlan, err := BuildPlan(ctx, ctasStmt, false)
		require.NoError(t, err)
		require.True(t, ctasPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetDefault().GetNullAbility())

		ambiguousSQL := "create view v_ambiguous as select n_nationkey from nation l join nation2 r " +
			"on l.n_nationkey = r.n_nationkey"
		rootCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: ambiguousSQL}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ambiguousSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(rootCtx, stmt, false)
		require.Error(t, err)
	})
}

func TestBuildCreateViewPreservesDefaultKinds(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols,
		&plan.ColDef{
			Name: "nullable_default_null",
			Typ:  plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: makePlan2NullConstExprWithType().Expr},
				OriginString: "null",
			},
		},
		&plan.ColDef{
			Name: "str_default",
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 20, NotNullable: true},
			Default: &plan.Default{
				Expr:         makePlan2StringConstExprWithType("seed"),
				OriginString: "'seed'",
			},
		},
		&plan.ColDef{
			Name: "expr_default",
			Typ:  plan.Type{Id: int32(types.T_uuid), NotNullable: true},
			Default: &plan.Default{
				Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_uuid), NotNullable: true}, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "uuid"},
				}}},
				OriginString: "(uuid())",
			},
		},
	)
	const rootSQL = "create view v_defaults as select nullable_default_null, str_default, expr_default from nation"
	rootCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: rootSQL}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(rootCtx, stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateView().GetTableDef().GetCols()
	require.Len(t, cols, 3)
	require.True(t, cols[0].GetDefault().GetNullAbility())
	require.Equal(t, "null", cols[0].GetDefault().GetOriginString())
	require.True(t, cols[0].GetDefault().GetExpr().GetLit().GetIsnull())
	require.False(t, cols[1].GetDefault().GetNullAbility())
	require.Equal(t, "'seed'", cols[1].GetDefault().GetOriginString())
	require.Equal(t, "seed", cols[1].GetDefault().GetExpr().GetLit().GetSval())
	require.False(t, cols[2].GetDefault().GetNullAbility())
	require.Equal(t, "(uuid())", cols[2].GetDefault().GetOriginString())
	require.Equal(t, "uuid", cols[2].GetDefault().GetExpr().GetF().GetFunc().GetObjName())
}

func TestGroupingExtensionsExposeNullableKeysInViewAndCTAS(t *testing.T) {
	newContext := func(rootSQL string) *rootSQLCompilerContext {
		ctx := NewMockCompilerContext(false)
		for _, name := range []string{"n_nationkey", "n_regionkey"} {
			col := ctx.tables["nation"].Cols[ctx.tables["nation"].Name2ColIndex[name]]
			col.Typ.NotNullable = true
			col.Default = &plan.Default{NullAbility: false}
		}
		return &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: rootSQL}
	}

	for _, test := range []struct {
		name     string
		groupBy  string
		nullable []bool
	}{
		{
			name:     "ordinary group by preserves source nullability",
			groupBy:  "n_nationkey, n_regionkey",
			nullable: []bool{false, false},
		},
		{
			name:     "rollup",
			groupBy:  "n_nationkey, n_regionkey with rollup",
			nullable: []bool{true, true},
		},
		{
			name:     "cube",
			groupBy:  "cube(n_nationkey, n_regionkey)",
			nullable: []bool{true, true},
		},
		{
			name:     "grouping sets",
			groupBy:  "grouping sets ((n_nationkey, n_regionkey), (n_nationkey), ())",
			nullable: []bool{true, true},
		},
		{
			name:     "grouping sets preserve keys active in every branch",
			groupBy:  "grouping sets ((n_nationkey, n_regionkey), (n_nationkey))",
			nullable: []bool{false, true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rootSQL := "create view grouping_extension_view as select n_nationkey, n_regionkey, count(*) as cnt from nation group by " + test.groupBy
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()

			viewPlan, err := BuildPlan(newContext(rootSQL), stmt, false)
			require.NoError(t, err)
			viewCols := viewPlan.GetDdl().GetCreateView().GetTableDef().GetCols()
			require.Len(t, viewCols, 3)
			for i, wantNullable := range test.nullable {
				require.Equal(t, wantNullable, viewCols[i].GetDefault().GetNullAbility(), viewCols[i].GetName())
			}
		})
	}

	for _, test := range []struct {
		name     string
		groupBy  string
		nullable []bool
	}{
		{
			name:     "rollup",
			groupBy:  "n_nationkey, n_regionkey with rollup",
			nullable: []bool{true, true},
		},
		{
			name:     "grouping sets preserve keys active in every branch",
			groupBy:  "grouping sets ((n_nationkey, n_regionkey), (n_nationkey))",
			nullable: []bool{false, true},
		},
	} {
		t.Run("CTAS "+test.name, func(t *testing.T) {
			ctasSQL := "create table grouping_extension_ctas as select n_nationkey, n_regionkey, count(*) as cnt from nation group by " + test.groupBy
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ctasSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()

			ctasPlan, err := BuildPlan(newContext(ctasSQL), stmt, false)
			require.NoError(t, err)
			ctasCols := ctasPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.GreaterOrEqual(t, len(ctasCols), 3)
			for i, wantNullable := range test.nullable {
				require.Equal(t, wantNullable, ctasCols[i].GetDefault().GetNullAbility(), ctasCols[i].GetName())
			}
		})
	}
}

func TestGroupingExtensionQueryOutputKeysAreNullable(t *testing.T) {
	for _, test := range []struct {
		name        string
		groupBy     string
		notNullable []bool
	}{
		{
			name:        "ordinary group by preserves source nullability",
			groupBy:     "n_nationkey, n_regionkey",
			notNullable: []bool{true, true},
		},
		{
			name:        "rollup",
			groupBy:     "n_nationkey, n_regionkey with rollup",
			notNullable: []bool{false, false},
		},
		{
			name:        "cube",
			groupBy:     "cube(n_nationkey, n_regionkey)",
			notNullable: []bool{false, false},
		},
		{
			name:        "grouping sets",
			groupBy:     "grouping sets ((n_nationkey, n_regionkey), (n_nationkey), ())",
			notNullable: []bool{false, false},
		},
		{
			name:        "grouping sets preserve keys active in every branch",
			groupBy:     "grouping sets ((n_nationkey, n_regionkey), (n_nationkey))",
			notNullable: []bool{true, false},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opt := NewMockOptimizer(false)
			ctx := opt.CurrentContext().(*MockCompilerContext)
			for _, name := range []string{"n_nationkey", "n_regionkey"} {
				ctx.tables["nation"].Cols[ctx.tables["nation"].Name2ColIndex[name]].Typ.NotNullable = true
			}

			queryPlan, err := runOneStmt(opt, t, "select n_nationkey, n_regionkey, count(*) as cnt from nation group by "+test.groupBy)
			require.NoError(t, err)
			query := queryPlan.GetQuery()
			rootNode := query.Nodes[query.Steps[len(query.Steps)-1]]
			for i, wantNotNullable := range test.notNullable {
				require.Equal(t, wantNotNullable, rootNode.ProjectList[i].Typ.NotNullable)
			}
		})
	}
}

func TestBuildCTASFromViewUsesIndependentExecutableDefault(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	sourceCol := ctx.tables["nation"].Cols[0]
	sourceCol.Typ.NotNullable = true
	sourceCol.Default = &plan.Default{
		NullAbility:  false,
		Expr:         makePlan2Int32ConstExprWithType(7),
		OriginString: "7",
	}
	decimalExpr, err := makePlan2DecimalExprWithType(t.Context(), "1.25")
	require.NoError(t, err)
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols,
		&plan.ColDef{
			Name: "str_col",
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 20, NotNullable: true},
			Default: &plan.Default{
				Expr:         makePlan2StringConstExprWithType("seed"),
				OriginString: "'seed'",
			},
		},
		&plan.ColDef{
			Name: "amount",
			Typ:  plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2, NotNullable: true},
			Default: &plan.Default{
				Expr:         decimalExpr,
				OriginString: "1.25",
			},
		},
		&plan.ColDef{
			Name: "priority",
			Typ:  plan.Type{Id: int32(types.T_enum), Enumvalues: "low,medium,high", NotNullable: true},
			Default: &plan.Default{
				Expr:         makePlan2StringConstExprWithType("medium"),
				OriginString: "'medium'",
			},
		},
		&plan.ColDef{
			Name: "flags",
			Typ:  plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b", NotNullable: true},
			Default: &plan.Default{
				Expr:         makePlan2Uint64ConstExprWithType(1),
				OriginString: "'a'",
			},
		},
		&plan.ColDef{
			Name: "nullable_col",
			Typ:  plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         makePlan2Int32ConstExprWithType(7),
				OriginString: "7",
			},
		},
		&plan.ColDef{
			Name: "expr_col",
			Typ:  plan.Type{Id: int32(types.T_uuid), NotNullable: true},
			Default: &plan.Default{
				Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_uuid), NotNullable: true}, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "uuid"},
				}}},
				OriginString: "(uuid())",
			},
		},
		&plan.ColDef{
			Name: "nullable_expr",
			Typ:  plan.Type{Id: int32(types.T_uuid)},
			Default: &plan.Default{
				NullAbility: true,
				Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_uuid)}, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "uuid"},
				}}},
				OriginString: "(uuid())",
			},
		},
	)
	const createViewSQL = "create view v_source_t as " +
		"select n_nationkey as qty, str_col, amount, priority, flags, nullable_col, expr_col, nullable_expr from nation"
	createCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createViewSQL}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createViewSQL, 1)
	require.NoError(t, err)
	viewPlan, err := BuildPlan(createCtx, stmt, false)
	stmt.Free()
	require.NoError(t, err)

	viewDef := DeepCopyTableDef(viewPlan.GetDdl().GetCreateView().GetTableDef(), true)
	require.Equal(t, "7", viewDef.GetCols()[0].GetDefault().GetOriginString())
	viewDef.Name = "v_source_t"
	viewDef.DbName = "tpch"
	viewDef.TableType = catalog.SystemViewRel
	ctx.tables[viewDef.Name] = viewDef
	ctx.objects[viewDef.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: viewDef.Name}

	for _, test := range []struct {
		name      string
		selectSQL string
	}{
		{name: "direct view", selectSQL: "select qty, str_col, amount, priority, flags, nullable_col, expr_col, nullable_expr from v_source_t"},
		{name: "view through derived", selectSQL: "select * from (select qty, str_col, amount, priority, flags, nullable_col, expr_col, nullable_expr from v_source_t) d"},
		{name: "view through cte", selectSQL: "with d as (select qty, str_col, amount, priority, flags, nullable_col, expr_col, nullable_expr from v_source_t) select * from d"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"create table copied as "+test.selectSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ctasPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			cols := ctasPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.GreaterOrEqual(t, len(cols), 8)
			for i, wantOrigin := range []string{"0", "''", "0.00", "'low'", "''", ""} {
				def := cols[i].GetDefault()
				require.NotNil(t, def)
				require.Equal(t, wantOrigin, def.GetOriginString(), cols[i].GetName())
				if i == 5 {
					require.True(t, def.GetNullAbility())
					require.Nil(t, def.GetExpr())
					continue
				}
				require.False(t, def.GetNullAbility())
				require.NotNil(t, def.GetExpr(), cols[i].GetName())
				require.Equal(t, cols[i].Typ.Id, def.GetExpr().Typ.Id, cols[i].GetName())
			}
			for _, i := range []int{6, 7} {
				def := cols[i].GetDefault()
				require.NotNil(t, def)
				require.Equal(t, "(uuid())", def.GetOriginString(), cols[i].GetName())
				require.Equal(t, i == 7, def.GetNullAbility(), cols[i].GetName())
				require.Equal(t, "uuid", def.GetExpr().GetF().GetFunc().GetObjName(), cols[i].GetName())
			}
		})
	}
}

func TestCTASViewDefaultPolicyMatrix(t *testing.T) {
	explicitDefault := &plan.Default{
		Expr:         makePlan2Int32ConstExprWithType(1),
		OriginString: "1",
	}
	expressionDefault := func(origin string, nullable bool) *plan.Default {
		return &plan.Default{
			NullAbility: nullable,
			Expr: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int32), NotNullable: !nullable},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "generated_default"},
				}},
			},
			OriginString: origin,
		}
	}

	for _, test := range []struct {
		name       string
		typ        plan.Type
		defaultDef *plan.Default
		wantPolicy CTASDefaultPolicy
		wantOrigin string
	}{
		{name: "date", typ: plan.Type{Id: int32(types.T_date), NotNullable: true}, defaultDef: explicitDefault},
		{name: "datetime", typ: plan.Type{Id: int32(types.T_datetime), NotNullable: true}, defaultDef: explicitDefault},
		{name: "time", typ: plan.Type{Id: int32(types.T_time), NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "'00:00:00'"},
		{name: "timestamp", typ: plan.Type{Id: int32(types.T_timestamp), NotNullable: true}, defaultDef: explicitDefault},
		{name: "year", typ: plan.Type{Id: int32(types.T_year), NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "'0000'"},
		{name: "binary", typ: plan.Type{Id: int32(types.T_binary), Width: 8, NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "''"},
		{name: "varbinary", typ: plan.Type{Id: int32(types.T_varbinary), Width: 8, NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "''"},
		{name: "float", typ: plan.Type{Id: int32(types.T_float32), NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "0"},
		{name: "double", typ: plan.Type{Id: int32(types.T_float64), NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "0"},
		{name: "bit", typ: plan.Type{Id: int32(types.T_bit), Width: 8, NotNullable: true}, defaultDef: explicitDefault, wantPolicy: CTASDefaultUseTypeDefault, wantOrigin: "0"},
		{name: "uuid expression", typ: plan.Type{Id: int32(types.T_uuid), NotNullable: true}, defaultDef: expressionDefault("(uuid())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "date expression", typ: plan.Type{Id: int32(types.T_date), NotNullable: true}, defaultDef: expressionDefault("(curdate())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "datetime expression", typ: plan.Type{Id: int32(types.T_datetime), NotNullable: true}, defaultDef: expressionDefault("(now())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "timestamp expression", typ: plan.Type{Id: int32(types.T_timestamp), NotNullable: true}, defaultDef: expressionDefault("(now())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "int expression", typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, defaultDef: expressionDefault("(1 + 2)", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "constant folded int expression", typ: plan.Type{Id: int32(types.T_int32), NotNullable: true}, defaultDef: &plan.Default{Expr: makePlan2Int32ConstExprWithType(3), OriginString: "(1 + 2)"}, wantPolicy: CTASDefaultInheritViewSource},
		{name: "decimal expression", typ: plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2, NotNullable: true}, defaultDef: expressionDefault("(1.25 + 1)", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "double expression", typ: plan.Type{Id: int32(types.T_float64), NotNullable: true}, defaultDef: expressionDefault("(1.5 + 1)", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "varchar expression", typ: plan.Type{Id: int32(types.T_varchar), Width: 40, NotNullable: true}, defaultDef: expressionDefault("(uuid())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "varbinary expression", typ: plan.Type{Id: int32(types.T_varbinary), Width: 40, NotNullable: true}, defaultDef: expressionDefault("(uuid())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "time expression", typ: plan.Type{Id: int32(types.T_time), NotNullable: true}, defaultDef: expressionDefault("('01:30:00')", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "year expression", typ: plan.Type{Id: int32(types.T_year), NotNullable: true}, defaultDef: expressionDefault("(2024)", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "blob expression", typ: plan.Type{Id: int32(types.T_blob), NotNullable: true}, defaultDef: expressionDefault("(blob_default())", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "text expression", typ: plan.Type{Id: int32(types.T_text), NotNullable: true}, defaultDef: expressionDefault("('seed')", false), wantPolicy: CTASDefaultInheritViewSource},
		{name: "nullable int expression", typ: plan.Type{Id: int32(types.T_int32)}, defaultDef: expressionDefault("(1 + 2)", true), wantPolicy: CTASDefaultInheritViewSource},
		{name: "nullable varchar expression", typ: plan.Type{Id: int32(types.T_varchar)}, defaultDef: expressionDefault("(uuid())", true), wantPolicy: CTASDefaultInheritViewSource},
		{name: "nullable blob expression", typ: plan.Type{Id: int32(types.T_blob)}, defaultDef: expressionDefault("(blob_default())", true), wantPolicy: CTASDefaultInheritViewSource},
		{name: "nullable text expression", typ: plan.Type{Id: int32(types.T_text)}, defaultDef: expressionDefault("('seed')", true), wantPolicy: CTASDefaultInheritViewSource},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := SourceColumnMetadata{Typ: test.typ, Default: DeepCopyDefault(test.defaultDef)}
			require.Equal(t, test.wantPolicy, ctasViewDefaultPolicy(metadata))
			origin, ok := ctasViewTypeDefaultOrigin(test.typ)
			if test.wantPolicy == CTASDefaultUseTypeDefault {
				require.True(t, ok)
				require.Equal(t, test.wantOrigin, origin)
			} else if test.wantPolicy == CTASDefaultNone && test.typ.NotNullable {
				require.False(t, ok)
			}
		})
	}
}

func TestBuildNullableLOBCTASDefaultFromOrigin(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	for _, oid := range []types.T{types.T_text, types.T_blob} {
		defaultDef, err := buildCTASDefaultFromOrigin(
			ctx, plan.Type{Id: int32(oid)}, true, "('seed')")
		require.NoError(t, err)
		require.True(t, defaultDef.GetNullAbility())
		require.Equal(t, "('seed')", defaultDef.GetOriginString())
		require.NotNil(t, defaultDef.GetExpr())
		require.Equal(t, int32(oid), defaultDef.GetExpr().Typ.Id)
	}
}

func addMySQLSpecialTypeColumns(ctx *MockCompilerContext) {
	ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols,
		&plan.ColDef{
			Name: "priority",
			Typ: plan.Type{
				Id:          int32(types.T_enum),
				Enumvalues:  "low,medium,high",
				NotNullable: true,
			},
		},
		&plan.ColDef{
			Name: "flags",
			Typ: plan.Type{
				Id:         int32(types.T_uint64),
				Enumvalues: "red,green,blue",
			},
		},
	)
}

func TestBuildCreateViewPreservesMySQLSpecialColumnTypes(t *testing.T) {
	const rootSQL = "create view v (renamed_priority, renamed_flags, renamed_name) as " +
		"select priority, flags, n_name from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	addMySQLSpecialTypeColumns(ctx.MockCompilerContext)
	priorityCol := ctx.tables["nation"].Cols[len(ctx.tables["nation"].Cols)-2]
	priorityCol.Default = &plan.Default{
		Expr:         makePlan2StringConstExprWithType("medium"),
		OriginString: "'medium'",
	}
	flagsCol := ctx.tables["nation"].Cols[len(ctx.tables["nation"].Cols)-1]
	flagsCol.Default = &plan.Default{
		NullAbility:  true,
		Expr:         makePlan2Uint64ConstExprWithType(1),
		OriginString: "'red'",
	}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateView().GetTableDef().GetCols()
	require.Len(t, cols, 3)
	priorityType := cols[0].GetTyp()
	flagsType := cols[1].GetTyp()
	nameType := cols[2].GetTyp()
	require.Equal(t, "renamed_priority", cols[0].GetName())
	require.Equal(t, int32(types.T_enum), priorityType.GetId())
	require.Equal(t, "low,medium,high", priorityType.GetEnumvalues())
	require.True(t, priorityType.GetNotNullable())
	require.Equal(t, "'medium'", cols[0].GetDefault().GetOriginString())
	require.Equal(t, "medium", cols[0].GetDefault().GetExpr().GetLit().GetSval())
	require.Equal(t, "renamed_flags", cols[1].GetName())
	require.Equal(t, int32(types.T_uint64), flagsType.GetId())
	require.Equal(t, "red,green,blue", flagsType.GetEnumvalues())
	require.False(t, flagsType.GetNotNullable())
	require.Equal(t, "'red'", cols[1].GetDefault().GetOriginString())
	require.Equal(t, uint64(1), cols[1].GetDefault().GetExpr().GetLit().GetU64Val())
	require.Equal(t, "renamed_name", cols[2].GetName())
	require.Equal(t, int32(types.T_varchar), nameType.GetId())
}

func TestBuildCreateViewTracksMySQLSpecialColumnTypeProvenance(t *testing.T) {
	tests := []struct {
		name            string
		selectSQL       string
		wantSpecialType bool
	}{
		{name: "direct", selectSQL: "select priority, flags from nation", wantSpecialType: true},
		{name: "order by", selectSQL: "select priority, flags from nation order by priority, flags", wantSpecialType: true},
		{name: "order by null", selectSQL: "select priority, flags from nation order by null", wantSpecialType: true},
		{name: "group by", selectSQL: "select priority, flags from nation group by priority, flags", wantSpecialType: true},
		{name: "distinct", selectSQL: "select distinct priority, flags from nation", wantSpecialType: true},
		{name: "derived table", selectSQL: "select priority, flags from (select priority, flags from nation) d", wantSpecialType: true},
		{name: "cte", selectSQL: "with d as (select priority, flags from nation) select priority, flags from d", wantSpecialType: true},
		{name: "derived table order by", selectSQL: "select priority, flags from (select priority, flags from nation) d order by flags", wantSpecialType: true},
		{name: "cte order by", selectSQL: "with d as (select priority, flags from nation) select priority, flags from d order by flags", wantSpecialType: true},
		{name: "alias", selectSQL: "select priority as p, flags as f from nation", wantSpecialType: true},
		{name: "same arms union distinct", selectSQL: "select priority, flags from nation union select priority, flags from nation"},
		{name: "union all", selectSQL: "select priority, flags from nation union all select priority, flags from nation"},
		{name: "recursive cte", selectSQL: "with recursive d(priority, flags) as (select priority, flags from nation union all select priority, flags from d where false) select priority, flags from d"},
		{name: "string expressions", selectSQL: "select concat(priority, ''), concat(flags, '') from nation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rootSQL := "create view v as " + test.selectSQL
			ctx := &rootSQLCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				rootSQL:             rootSQL,
			}
			addMySQLSpecialTypeColumns(ctx.MockCompilerContext)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()

			viewPlan, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			cols := viewPlan.GetDdl().GetCreateView().GetTableDef().GetCols()
			require.Len(t, cols, 2)
			if test.wantSpecialType {
				require.True(t, isEnumPlanType(&cols[0].Typ))
				require.True(t, isSetPlanType(&cols[1].Typ))
			} else {
				require.Equal(t, int32(types.T_varchar), cols[0].Typ.GetId())
				require.Equal(t, int32(types.T_varchar), cols[1].Typ.GetId())
			}
		})
	}
}

func TestBuildCTASPreservesMySQLSpecialColumnTypes(t *testing.T) {
	const sql = "create table copied as select priority, flags, n_name from nation"
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 3)
	require.True(t, isEnumPlanType(&cols[0].Typ))
	require.Equal(t, "low,medium,high", cols[0].Typ.GetEnumvalues())
	require.True(t, cols[0].Typ.GetNotNullable())
	require.False(t, cols[0].GetDefault().GetNullAbility())
	require.True(t, isSetPlanType(&cols[1].Typ))
	require.Equal(t, "red,green,blue", cols[1].Typ.GetEnumvalues())
	require.Equal(t, int32(types.T_varchar), cols[2].Typ.GetId())
}

func TestBuildCTASPreservesLosslessBinaryResultDomains(t *testing.T) {
	const sql = `create table copied as select
		convert(cast(1 as signed) using binary) converted,
		char(65, 66) default_char,
		char(65 using utf8mb4) text_char,
		repeat(X'61', 70000) repeated,
		concat(cast(X'61' as binary(65535)), X'62') concatenated,
		replace(cast(repeat('a', 40000) as text), 'a', 'bb') expanded_text`
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 6)

	require.Equal(t, int32(types.T_varbinary), cols[0].Typ.Id)
	require.Equal(t, int32(20), cols[0].Typ.Width)
	require.Equal(t, uint32(types.CharsetBinary), cols[0].Typ.Charset)
	require.Equal(t, int32(types.T_varbinary), cols[1].Typ.Id)
	require.Equal(t, int32(8), cols[1].Typ.Width)
	require.Equal(t, int32(types.T_varchar), cols[2].Typ.Id)
	require.Equal(t, int32(types.T_blob), cols[3].Typ.Id)
	require.Equal(t, int32(types.T_blob), cols[4].Typ.Id)
	require.Equal(t, int32(types.T_text), cols[5].Typ.Id)
}

func TestBuildCTASNarrowsKnownExpandingStringResults(t *testing.T) {
	const sql = `create table bounded as select
		repeat('a', 2) repeated,
		lpad('a', 2, 'b') left_padded,
		rpad('a', 2, 'b') right_padded,
		replace('a', 'a', 'b') replaced,
		insert('a', 1, 0, 'b') inserted,
		replace(X'61', X'61', X'62') binary_replaced,
		insert(X'61', 1, 0, X'62') binary_inserted,
		reverse(space(500) + space(600)) reversed_text,
		reverse('123' + space(1) + '456') chained_text,
		repeat(coalesce(X'F09F9880', X'61'), 2) binary_charset_repeated,
		lpad(coalesce(X'F09F9880', X'61'), 2, X'62') binary_charset_padded`
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 11)
	for _, index := range []int{0, 1, 2, 3, 4} {
		require.Equal(t, int32(types.T_varchar), cols[index].Typ.Id, cols[index].Name)
		require.LessOrEqual(t, cols[index].Typ.Width, int32(types.MaxVarcharLen), cols[index].Name)
	}
	for _, index := range []int{5, 6} {
		require.Equal(t, int32(types.T_varbinary), cols[index].Typ.Id, cols[index].Name)
		require.LessOrEqual(t, cols[index].Typ.Width, int32(types.MaxVarBinaryLen), cols[index].Name)
	}
	require.Equal(t, int32(types.T_text), cols[7].Typ.Id)
	require.Equal(t, int32(types.T_text), cols[8].Typ.Id)
	for _, index := range []int{9, 10} {
		require.Equal(t, int32(types.T_varbinary), cols[index].Typ.Id)
		require.Equal(t, int32(8), cols[index].Typ.Width)
	}
}

func TestBuildCTASPreservesFormattedScalarBounds(t *testing.T) {
	const sql = `create table formatted_bounds as select
		convert(cast(-0.99 as decimal(2,2)) using binary) decimal_binary,
		concat(cast(1 as signed), cast(2 as signed)) concatenated,
		quote(cast(1 as signed)) quoted`
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 3)
	require.Equal(t, int32(types.T_varbinary), cols[0].Typ.Id)
	require.Equal(t, int32(5), cols[0].Typ.Width)
	require.Equal(t, int32(types.T_varchar), cols[1].Typ.Id)
	require.Equal(t, int32(40), cols[1].Typ.Width)
	require.Equal(t, int32(types.T_varchar), cols[2].Typ.Id)
	require.Equal(t, int32(42), cols[2].Typ.Width)
}

func TestViewRebindPreservesMySQLSpecialColumnSemantics(t *testing.T) {
	const createViewSQL = "create view v_enum_set as select priority, flags, n_name from nation"
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	createCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createViewSQL}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createViewSQL, 1)
	require.NoError(t, err)
	createPlan, err := BuildPlan(createCtx, stmt, false)
	stmt.Free()
	require.NoError(t, err)

	viewDef := DeepCopyTableDef(createPlan.GetDdl().GetCreateView().GetTableDef(), true)
	viewDef.Name = "v_enum_set"
	viewDef.DbName = "tpch"
	viewDef.TableType = catalog.SystemViewRel
	ctx.tables["v_enum_set"] = viewDef
	ctx.objects["v_enum_set"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v_enum_set"}

	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select priority from v_enum_set order by priority", 1)
	require.NoError(t, err)
	selectPlan, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)

	var sortKey *plan.Expr
	for _, node := range selectPlan.GetQuery().GetNodes() {
		if node.GetNodeType() == plan.Node_SORT {
			require.Len(t, node.GetOrderBy(), 1)
			sortKey = node.GetOrderBy()[0].GetExpr()
			break
		}
	}
	require.NotNil(t, sortKey)
	sortType := sortKey.GetTyp()
	require.Equal(t, int32(types.T_enum), sortType.GetId())
	require.Equal(t, "low,medium,high", sortType.GetEnumvalues())
	query := selectPlan.GetQuery()
	require.Len(t, query.GetSteps(), 1)
	resultNode := query.GetNodes()[query.GetSteps()[0]]
	require.Len(t, resultNode.GetProjectList(), 1)
	resultType := resultNode.GetProjectList()[0].GetTyp()
	require.Equal(t, int32(types.T_varchar), resultType.GetId())

	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select flags from v_enum_set", 1)
	require.NoError(t, err)
	rawSetPlan, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	setDisplayFound := false
	for _, node := range rawSetPlan.GetQuery().GetNodes() {
		for _, project := range node.GetProjectList() {
			fn := project.GetF()
			if fn == nil {
				continue
			}
			require.NotEqual(t, moSetCastValueToIndexFun, fn.GetFunc().GetObjName(),
				"a direct view projection must not round-trip a SET bitmap through its display string")
			if fn.GetFunc().GetObjName() == moSetCastIndexToValueFun {
				setDisplayFound = true
				require.Len(t, fn.GetArgs(), 2)
				require.True(t, isSetPlanType(&fn.GetArgs()[1].Typ))
			}
		}
	}
	require.True(t, setDisplayFound)

	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table copied_from_view as select priority, flags, n_name from v_enum_set", 1)
	require.NoError(t, err)
	ctasPlan, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	cols := ctasPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 3)
	require.True(t, isEnumPlanType(&cols[0].Typ))
	require.Equal(t, "low,medium,high", cols[0].Typ.GetEnumvalues())
	require.True(t, isSetPlanType(&cols[1].Typ))
	require.Equal(t, "red,green,blue", cols[1].Typ.GetEnumvalues())
	require.Equal(t, int32(types.T_varchar), cols[2].Typ.GetId())

	ctasDef := DeepCopyTableDef(ctasPlan.GetDdl().GetCreateTable().GetTableDef(), true)
	ctasDef.Name = "copied_from_view"
	ctasDef.DbName = "tpch"
	ctx.tables[ctasDef.Name] = ctasDef
	ctx.objects[ctasDef.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: ctasDef.Name}
	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		ctasPlan.GetDdl().GetCreateTable().GetCreateAsSelectSql(), 1)
	require.NoError(t, err)
	insertPlan, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	for _, node := range insertPlan.GetQuery().GetNodes() {
		for _, project := range node.GetProjectList() {
			if fn := project.GetF(); fn != nil {
				require.NotEqual(t, moSetCastValueToIndexFun, fn.GetFunc().GetObjName(),
					"CTAS INSERT must retain the projected SET bitmap: node=%d type=%s expr=%s",
					node.GetNodeId(), node.GetNodeType().String(), project.String())
			}
		}
	}

	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"insert into copied_from_view (priority, flags, n_name) "+
			"select priority, concat(flags, ',green'), n_name from v_enum_set", 1)
	require.NoError(t, err)
	nestedPlan, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	nestedDisplayFound := false
	for _, node := range nestedPlan.GetQuery().GetNodes() {
		for _, project := range node.GetProjectList() {
			walkPlanExpr(project, func(expr *plan.Expr) {
				if fn := expr.GetF(); fn != nil && fn.GetFunc().GetObjName() == moSetCastIndexToValueFun {
					nestedDisplayFound = true
				}
			})
		}
	}
	require.True(t, nestedDisplayFound,
		"a SET column nested in CONCAT must keep its SQL-visible string semantics")
}

func TestViewRebindPreservesTransparentMySQLSpecialColumnTypes(t *testing.T) {
	tests := []struct {
		name            string
		selectSQL       string
		wantSpecialType bool
	}{
		{name: "derived table", selectSQL: "select priority, flags from (select priority, flags from nation) d", wantSpecialType: true},
		{name: "cte", selectSQL: "with d as (select priority, flags from nation) select priority, flags from d", wantSpecialType: true},
		{name: "order by", selectSQL: "select priority, flags from nation order by flags", wantSpecialType: true},
		{name: "derived table order by", selectSQL: "select priority, flags from (select priority, flags from nation) d order by flags", wantSpecialType: true},
		{name: "cte order by", selectSQL: "with d as (select priority, flags from nation) select priority, flags from d order by flags", wantSpecialType: true},
		{name: "union all", selectSQL: "select priority, flags from nation union all select priority, flags from nation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			createViewSQL := "create view v as " + test.selectSQL
			ctx := NewMockCompilerContext(false)
			addMySQLSpecialTypeColumns(ctx)
			createCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createViewSQL}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createViewSQL, 1)
			require.NoError(t, err)
			createPlan, err := BuildPlan(createCtx, stmt, false)
			stmt.Free()
			require.NoError(t, err)

			viewDef := DeepCopyTableDef(createPlan.GetDdl().GetCreateView().GetTableDef(), true)
			viewDef.Name = "v"
			viewDef.DbName = "tpch"
			viewDef.TableType = catalog.SystemViewRel
			ctx.tables[viewDef.Name] = viewDef
			ctx.objects[viewDef.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: viewDef.Name}

			stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
				"create table copied as select priority, flags from v", 1)
			require.NoError(t, err)
			ctasPlan, err := BuildPlan(ctx, stmt, false)
			stmt.Free()
			require.NoError(t, err)
			cols := ctasPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.GreaterOrEqual(t, len(cols), 2)
			if test.wantSpecialType {
				require.True(t, isEnumPlanType(&cols[0].Typ))
				require.True(t, isSetPlanType(&cols[1].Typ))
				for _, node := range ctasPlan.GetQuery().GetNodes() {
					for _, project := range node.GetProjectList() {
						walkPlanExpr(project, func(expr *plan.Expr) {
							if fn := expr.GetF(); fn != nil {
								require.NotEqual(t, moSetCastValueToIndexFun, fn.GetFunc().GetObjName(),
									"transparent View CTAS must not round-trip a SET bitmap")
							}
						})
					}
				}

				stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
					"select cast(flags as unsigned) from v", 1)
				require.NoError(t, err)
				castPlan, err := BuildPlan(ctx, stmt, false)
				stmt.Free()
				require.NoError(t, err)
				for _, node := range castPlan.GetQuery().GetNodes() {
					for _, project := range node.GetProjectList() {
						walkPlanExpr(project, func(expr *plan.Expr) {
							if fn := expr.GetF(); fn != nil {
								require.NotEqual(t, moSetCastIndexToValueFun, fn.GetFunc().GetObjName(),
									"numeric View consumer must receive the raw SET bitmap")
							}
						})
					}
				}
			} else {
				require.Equal(t, int32(types.T_varchar), cols[0].Typ.GetId())
				require.Equal(t, int32(types.T_varchar), cols[1].Typ.GetId())
			}
		})
	}
}

func TestViewSpecialTypeBoundaryCanonicalizesSemanticResults(t *testing.T) {
	for _, test := range []struct {
		name      string
		selectSQL string
	}{
		{name: "distinct", selectSQL: "select distinct flags from nation"},
		{name: "group by", selectSQL: "select flags from nation group by flags"},
		{name: "group by order", selectSQL: "select flags from nation group by flags order by flags"},
		{name: "derived distinct", selectSQL: "select flags from (select distinct flags from nation) d"},
	} {
		t.Run(test.name, func(t *testing.T) {
			createViewSQL := "create view v_semantic_set as " + test.selectSQL
			ctx := NewMockCompilerContext(false)
			addMySQLSpecialTypeColumns(ctx)
			createCtx := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createViewSQL}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createViewSQL, 1)
			require.NoError(t, err)
			createPlan, err := BuildPlan(createCtx, stmt, false)
			stmt.Free()
			require.NoError(t, err)

			viewDef := DeepCopyTableDef(createPlan.GetDdl().GetCreateView().GetTableDef(), true)
			viewDef.Name = "v_semantic_set"
			viewDef.DbName = "tpch"
			viewDef.TableType = catalog.SystemViewRel
			ctx.tables[viewDef.Name] = viewDef
			ctx.objects[viewDef.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: viewDef.Name}

			stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL, "select flags from v_semantic_set", 1)
			require.NoError(t, err)
			queryPlan, err := BuildPlan(ctx, stmt, false)
			stmt.Free()
			require.NoError(t, err)

			setDisplayProjects := 0
			setCanonicalProjects := 0
			semanticStringInput := false
			for _, node := range queryPlan.GetQuery().GetNodes() {
				if node.GetNodeType() == plan.Node_AGG {
					for _, group := range node.GetGroupBy() {
						if types.T(group.Typ.Id).IsMySQLString() {
							semanticStringInput = true
						}
					}
				}
				for _, project := range node.GetProjectList() {
					if fn := project.GetF(); fn != nil {
						switch fn.GetFunc().GetObjName() {
						case moSetCastIndexToValueFun:
							setDisplayProjects++
						case moSetCastValueToIndexFun:
							setCanonicalProjects++
						}
					}
				}
			}
			require.GreaterOrEqual(t, setDisplayProjects, 1,
				"semantic operator must consume the SQL-visible SET value")
			require.True(t, semanticStringInput,
				"GROUP BY/DISTINCT must operate on the SQL-visible string type")
			require.GreaterOrEqual(t, setCanonicalProjects, 1,
				"completed semantic View boundary must canonically re-encode SET")
			require.True(t, isSetPlanType(&viewDef.Cols[0].Typ))
		})
	}
}

func TestOutputColumnProvenanceCarriesSourceAndClearsSemanticBoundaries(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	ctx.tables["nation"].Cols[0].Default = &plan.Default{
		Expr:         makePlan2Int32ConstExprWithType(7),
		OriginString: "7",
	}

	tests := []struct {
		name              string
		sql               string
		wantState         ProvenanceState
		wantDefault       string
		ctasDefaultPolicy CTASDefaultPolicy
	}{
		{name: "direct", sql: "select n_nationkey from nation", wantState: ProvenanceSingleSource, wantDefault: "7", ctasDefaultPolicy: CTASDefaultInheritSource},
		{name: "alias derived", sql: "select k from (select n_nationkey as k from nation) d", wantState: ProvenanceSingleSource, wantDefault: "7"},
		{name: "non recursive cte", sql: "with d as (select n_nationkey as k from nation) select k from d", wantState: ProvenanceSingleSource, wantDefault: "7"},
		{name: "expression", sql: "select n_nationkey + 0 from nation", wantState: ProvenanceNone},
		{name: "same arms union distinct", sql: "select n_nationkey from nation union select n_nationkey from nation", wantState: ProvenanceNone},
		{name: "union all", sql: "select n_nationkey from nation union all select n_nationkey from nation", wantState: ProvenanceNone},
		{name: "recursive cte", sql: "with recursive d(k) as (select n_nationkey from nation union all select k from d where false) select k from d", wantState: ProvenanceNone},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			selectStmt := stmt.(*tree.Select)
			builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
			bindCtx := NewBindContext(builder, nil)
			_, err = builder.bindSelect(selectStmt, bindCtx, true)
			require.NoError(t, err)

			provenance := bindCtx.outputColumnProvenanceForProject(0)
			require.Equal(t, test.wantState, provenance.State)
			if test.wantState == ProvenanceSingleSource {
				require.NotNil(t, provenance.Source)
				require.Equal(t, test.wantDefault, provenance.Source.Metadata.Default.GetOriginString())
				require.Equal(t, test.ctasDefaultPolicy, provenance.CTASDefaultPolicy)
				require.NotZero(t, provenance.Source.RelPos)
			} else {
				require.Nil(t, provenance.Source)
			}
		})
	}
}

func TestBuildCTASConsumesOutputColumnProvenance(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	sourceDefault := &plan.Default{
		Expr:         makePlan2Int32ConstExprWithType(7),
		OriginString: "7",
	}
	ctx.tables["nation"].Cols[0].Default = sourceDefault

	tests := []struct {
		name        string
		selectSQL   string
		wantDefault string
	}{
		{name: "direct alias", selectSQL: "select n_nationkey as k from nation", wantDefault: "7"},
		{name: "derived", selectSQL: "select k from (select n_nationkey as k from nation) d"},
		{name: "cte", selectSQL: "with d as (select n_nationkey as k from nation) select k from d"},
		{name: "expression", selectSQL: "select n_nationkey + 0 as k from nation"},
		{name: "union", selectSQL: "select n_nationkey as k from nation union all select n_nationkey from nation"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sql := "create table copied as " + test.selectSQL
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, cols)
			require.Equal(t, test.wantDefault, cols[0].GetDefault().GetOriginString())
			if test.wantDefault == "" {
				require.Nil(t, cols[0].GetDefault().GetExpr())
			} else {
				require.Equal(t, int32(7), cols[0].GetDefault().GetExpr().GetLit().GetI32Val())
				require.NotSame(t, sourceDefault.Expr, cols[0].GetDefault().GetExpr())
			}
		})
	}
}

func TestOutputColumnProvenanceSnapshotsCatalogMetadataOnce(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	priorityCol := ctx.tables["nation"].Cols[len(ctx.tables["nation"].Cols)-2]
	priorityCol.Default = &plan.Default{
		Expr:         makePlan2StringConstExprWithType("low"),
		OriginString: "'low'",
	}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select priority from nation", 1)
	require.NoError(t, err)
	defer stmt.Free()
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	_, err = builder.bindSelect(stmt.(*tree.Select), bindCtx, true)
	require.NoError(t, err)
	provenance := bindCtx.outputColumnProvenanceForProject(0)
	require.Equal(t, ProvenanceSingleSource, provenance.State)
	require.NotNil(t, provenance.Source)

	priorityCol.Typ.Enumvalues = "changed"
	priorityCol.Default.OriginString = "'changed'"
	priorityCol.Default.Expr.GetLit().Value = &plan.Literal_Sval{Sval: "changed"}
	require.Equal(t, "low,medium,high", provenance.Source.Metadata.Typ.Enumvalues)
	require.NotNil(t, provenance.Source.Metadata.Default)
	require.Equal(t, "'low'", provenance.Source.Metadata.Default.GetOriginString())
	require.Equal(t, "low", provenance.Source.Metadata.Default.GetExpr().GetLit().GetSval())
}

func TestTransparentOutputSourceExprRejectsSemanticExpressions(t *testing.T) {
	enumType := plan.Type{Id: int32(types.T_enum), Enumvalues: "low,high"}
	valid := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: moEnumCastIndexToValueFun},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Typ: enumType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}}},
			},
		}},
	}

	got, ok := transparentOutputSourceExpr(valid)
	require.True(t, ok)
	require.Equal(t, enumType, got.Typ)

	for _, mutate := range []func(*plan.Expr){
		func(expr *plan.Expr) { expr.GetF().Args = expr.GetF().Args[:1] },
		func(expr *plan.Expr) { expr.GetF().Args[1].Expr = nil },
		func(expr *plan.Expr) { expr.GetF().Args[1].Typ.Id = int32(types.T_varchar) },
		func(expr *plan.Expr) { expr.GetF().Func.ObjName = "concat" },
	} {
		expr := DeepCopyExpr(valid)
		mutate(expr)
		_, ok = transparentOutputSourceExpr(expr)
		require.False(t, ok)
	}
}

func TestBuildCreateViewRejectsTemporaryTable(t *testing.T) {
	tests := []string{
		"create view v as select * from nation",
		"create view v as select 1 from nation where false",
		"create view v as select * from (select * from nation) n",
		"create view v as select (select n_name from nation limit 1)",
		"create view v as (select * from nation)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.tables["nation"].IsTemporary = true

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrViewSelectTmpTable))
			require.Equal(t, uint16(moerr.ER_VIEW_SELECT_TMPTABLE), err.(*moerr.Error).MySQLCode())
			require.Equal(t, "View's SELECT refers to a temporary table 'nation'", err.Error())
		})
	}
}

func TestBuildTemporaryTableMarksCatalogRelkind(t *testing.T) {
	const rootSQL = "create temporary table temp_marked (id int, unique key uk_id (id))"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	createTable := p.GetDdl().GetCreateTable()
	require.NotNil(t, createTable)
	require.NotEmpty(t, createTable.IndexTables)

	requireTemporaryCatalogRelkind(t, createTable.TableDef)
	for _, tableDef := range createTable.IndexTables {
		requireIndexCatalogRelkind(t, tableDef)
	}

	require.Equal(t, rootSQL, tableDefCreateSQL(createTable.TableDef))
}

func TestBuildCreateTablePreservesSingleStatementSQL(t *testing.T) {
	const rootSQL = "/* before */ CREATE TABLE /* table */ t_check (id INT, CONSTRAINT chk_id CHECK (id > 0));"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, rootSQL, tableDefCreateSQL(p.GetDdl().GetCreateTable().GetTableDef()))
}

func TestBuildCreateTableLikePersistsExpandedSQL(t *testing.T) {
	const rootSQL = "CREATE TABLE legacy_clone LIKE legacy_source"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.tables["legacy_source"] = &plan.TableDef{
		Name:      "legacy_source",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_source(payload TINYTEXT)",
		Cols: []*plan.ColDef{{
			Name: "payload", OriginName: "payload", Seqnum: 0,
			Typ: plan.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen},
			Default: &plan.Default{
				NullAbility: true,
			},
		}},
	}
	ctx.objects["legacy_source"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "legacy_source"}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	persisted := tableDefCreateSQL(built.GetDdl().GetCreateTable().GetTableDef())
	require.NotContains(t, strings.ToUpper(persisted), " LIKE ")
	require.Contains(t, strings.ToUpper(persisted), "TINYTEXT")
}

func TestBuildCreateTableLikeRestoresSubscriptionBeforePlanningTarget(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			const rootSQL = "CREATE TABLE localdb.clone LIKE subdb.source"
			base := NewMockCompilerContext(false)
			base.dbs["localdb"] = true
			base.dbs["subdb"] = true
			base.tables["source"] = &plan.TableDef{
				Name:      "source",
				TableType: catalog.SystemOrdinaryRel,
				Cols: []*plan.ColDef{{
					Name: "id", OriginName: "id",
					Typ:     plan.Type{Id: int32(types.T_int32)},
					Default: &plan.Default{NullAbility: true},
				}},
			}
			base.objects["source"] = &plan.ObjectRef{
				SchemaName:       "publisherdb",
				ObjName:          "source",
				SubscriptionName: "subdb",
				PubInfo:          &plan.PubInfo{TenantId: 7},
			}
			ctx := &subscriptionScopeCompilerContext{
				MockCompilerContext: base,
				subscription: &SubscriptionMeta{
					AccountId: 7, DbName: "publisherdb", SubName: "subdb",
					Tables: "*",
				},
			}

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, prepared)
			require.NoError(t, err)
			require.Equal(t, "localdb", built.GetDdl().GetCreateTable().GetDatabase())
			require.Nil(t, ctx.GetQueryingSubscription())
		})
	}
}

func TestBuildCreateTableLikeSubscriptionForeignKeysUseSourceOnlyContext(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		foreignTbl uint64
	}{
		{name: "self reference", foreignTbl: 0},
		{name: "other table", foreignTbl: 101},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const rootSQL = "CREATE TABLE localdb.child_copy LIKE subdb.child"
			base := NewMockCompilerContext(false)
			base.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
				if name == "foreign_key_checks" {
					return int64(0), nil
				}
				return nil, moerr.NewInternalError(t.Context(), fmt.Sprintf("unexpected variable %s", name))
			}
			base.dbs["localdb"] = true
			base.dbs["subdb"] = true
			parent := &plan.TableDef{
				Name: "parent", DbName: "publisherdb", TblId: 101,
				Cols: []*plan.ColDef{{
					ColId: 1, Name: "id", OriginName: "id",
					Typ: plan.Type{Id: int32(types.T_int32)},
				}},
				Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
			}
			child := &plan.TableDef{
				Name: "child", DbName: "publisherdb", TblId: 102,
				Cols: []*plan.ColDef{{
					ColId: 2, Name: "parent_id", OriginName: "parent_id",
					Typ:     plan.Type{Id: int32(types.T_int32)},
					Default: &plan.Default{NullAbility: true},
				}},
				Fkeys: []*plan.ForeignKeyDef{{
					Name: "fk_parent", Cols: []uint64{2}, ForeignTbl: testCase.foreignTbl,
					ForeignCols: []uint64{1},
				}},
			}
			if testCase.foreignTbl == 0 {
				child.Cols[0].ColId = 1
				child.Fkeys[0].Cols = []uint64{1}
				child.Pkey = &plan.PrimaryKeyDef{Names: []string{"parent_id"}}
			}
			base.tables["child"] = child
			base.objects["child"] = &plan.ObjectRef{
				SchemaName: "publisherdb", ObjName: "child", SubscriptionName: "subdb",
				PubInfo: &plan.PubInfo{TenantId: 7},
			}
			base.tables["parent"] = parent
			base.objects["parent"] = &plan.ObjectRef{
				SchemaName: "publisherdb", ObjName: "parent", SubscriptionName: "subdb",
				PubInfo: &plan.PubInfo{TenantId: 7},
			}
			ctx := &subscriptionScopeCompilerContext{
				MockCompilerContext: base,
				subscription: &SubscriptionMeta{
					AccountId: 7, DbName: "publisherdb", SubName: "subdb", Tables: "*",
				},
				publisherByID: map[uint64]*TableDef{101: parent},
			}

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			require.Equal(t, "localdb", built.GetDdl().GetCreateTable().GetDatabase())
			require.Nil(t, ctx.GetQueryingSubscription())
		})
	}
}

func TestConstructCreateTableSQLSubscriptionCloneMapsPublisherForeignKeyToTarget(t *testing.T) {
	base := NewMockCompilerContext(false)
	base.dbs["clone_fk_chain"] = true
	parent := &plan.TableDef{
		Name: "parent", DbName: "publisherdb", TblId: 101,
		Cols: []*plan.ColDef{{
			ColId: 1, Name: "id", OriginName: "id",
			Typ: plan.Type{Id: int32(types.T_int32)},
		}},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
	}
	child := &plan.TableDef{
		Name: "child", DbName: "clone_fk_chain", TblId: 102,
		Cols: []*plan.ColDef{{
			ColId: 2, Name: "parent_id", OriginName: "parent_id",
			Typ:     plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: true},
		}},
		Fkeys: []*plan.ForeignKeyDef{{
			Name: "fk_parent", Cols: []uint64{2}, ForeignTbl: 101, ForeignCols: []uint64{1},
		}},
	}
	base.tables["parent"] = parent
	base.objects["parent"] = &plan.ObjectRef{SchemaName: "clone_fk_chain", ObjName: "parent"}
	subscription := &SubscriptionMeta{
		AccountId: 7, DbName: "publisherdb", SubName: "sub_fk_chain", Tables: "*",
	}
	ctx := &subscriptionScopeCompilerContext{
		MockCompilerContext: base,
		subscription:        subscription,
		publisherByID:       map[uint64]*TableDef{101: parent},
	}
	cloneStmt := &tree.CloneTable{
		SrcTable: *tree.NewTableName("child", tree.ObjectNamePrefix{
			SchemaName: "sub_fk_chain", ExplicitSchema: true,
		}, nil),
		StmtType: tree.WithinAccCloneDB,
	}

	createSQL, statement, err := constructCreateTableSQL(
		ctx, child, nil, true, cloneStmt, true, subscription,
	)
	require.NoError(t, err)
	defer statement.Free()
	require.Contains(t, createSQL, "REFERENCES `clone_fk_chain`.`parent`")
	require.NotContains(t, createSQL, "`publisherdb`.`parent`")
	require.Nil(t, ctx.GetQueryingSubscription())
}

func TestBuildCreateTableLikeAndCloneReconcileLegacyIndexVisibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "like", sql: "CREATE TABLE visibility_like LIKE legacy_visibility_source"},
		{name: "clone", sql: "CREATE TABLE visibility_clone CLONE legacy_visibility_source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			const sourceName = "legacy_visibility_source"
			const sourceID = 272464

			source := DeepCopyTableDef(ctx.tables["test_idx"], true)
			source.Name = sourceName
			source.DbName = ctx.DefaultDatabase()
			source.TblId = sourceID
			source.Indexes = []*plan.IndexDef{
				{
					IndexName:  "idx_legacy_visible",
					Parts:      []string{"n_name"},
					TableExist: true,
					Visible:    false,
				},
				{
					IndexName:  "idx_invisible",
					Parts:      []string{"n_name"},
					TableExist: true,
					Visible:    false,
				},
				{
					IndexName:  "idx_stale_marker",
					Parts:      []string{"n_name"},
					TableExist: true,
					Visible:    true,
					Option: &plan.IndexOption{
						Visibility: plan.IndexOption_VISIBILITY_VISIBLE,
					},
				},
			}
			ctx.tables[sourceName] = source
			ctx.objects[sourceName] = &plan.ObjectRef{
				SchemaName: ctx.DefaultDatabase(),
				ObjName:    sourceName,
				Obj:        sourceID,
			}

			proc := testutil.NewProc(t)
			proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
			ctx.GetProcessFunc = func() *process.Process { return proc }
			visibilityQueries := 0
			moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
				moruntime.InternalSQLExecutor,
				executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					if sql != "SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = 272464" {
						return executor.Result{}, nil
					}
					visibilityQueries++
					result := executor.NewMemResult(
						[]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp(),
					)
					result.NewBatchWithRowCount(3)
					require.NoError(t, executor.AppendStringRows(result, 0,
						[]string{"idx_legacy_visible", "idx_invisible", "idx_stale_marker"}))
					require.NoError(t, executor.AppendFixedRows(result, 1, []int8{1, 0, 0}))
					return result.GetResult(), nil
				}),
			)

			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			require.Equal(t, 1, visibilityQueries)

			createPlan := built
			if clone := built.GetDdl().GetCloneTable(); clone != nil {
				createPlan = clone.GetCreateTable()
			}
			createTable := createPlan.GetDdl().GetCreateTable()
			require.NotNil(t, createTable)
			visibility := make(map[string]bool, len(createTable.TableDef.Indexes))
			for _, indexDef := range createTable.TableDef.Indexes {
				visibility[indexDef.IndexName] = indexDef.Visible
				_, isSet := catalog.GetIndexVisibility(indexDef)
				require.True(t, isSet)
			}
			require.True(t, visibility["idx_legacy_visible"])
			require.False(t, visibility["idx_invisible"])
			require.False(t, visibility["idx_stale_marker"])
			persistedSQL := strings.ToUpper(tableDefCreateSQL(createTable.TableDef))
			require.Contains(t, persistedSQL, "IDX_INVISIBLE")
			require.Equal(t, 2, strings.Count(persistedSQL, " INVISIBLE"))
		})
	}
}

func TestRunSqlWithSnapshotUsesSourceTenant(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := testutil.NewProc(t)
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
	ctx.GetProcessFunc = func() *process.Process { return proc }

	const sourceTenant = uint32(42)
	var capturedAccountID uint32
	var capturedContextAccountID uint32
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		&captureSQLExecutor{exec: func(
			execCtx context.Context,
			_ string,
			opts executor.Options,
		) (executor.Result, error) {
			capturedAccountID = opts.AccountID()
			capturedContextAccountID, _ = defines.GetAccountId(execCtx)
			return executor.Result{}, nil
		}},
	)

	result, err := runSqlWithSnapshot(ctx, "select 1", &Snapshot{
		Tenant: &SnapshotTenant{TenantID: sourceTenant},
	})
	require.NoError(t, err)
	defer result.Close()
	require.Equal(t, sourceTenant, capturedAccountID)
	require.Equal(t, sourceTenant, capturedContextAccountID)
}

func TestBuildCreateTableLikeAndCloneRejectsSequenceSource(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	const sequenceSQL = "CREATE SEQUENCE seq1 INCREMENT 2 START WITH 11 NO CYCLE"

	sequenceStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sequenceSQL, 1)
	require.NoError(t, err)
	defer sequenceStmt.Free()
	sequencePlan, err := BuildPlan(ctx, sequenceStmt, false)
	require.NoError(t, err)

	sequenceDef := DeepCopyTableDef(
		sequencePlan.GetDdl().GetCreateSequence().GetTableDef(),
		true,
	)
	sequenceDef.DbName = ctx.DefaultDatabase()
	// The sequence builder stores relkind in the properties; catalog resolution
	// exposes it as TableType on the resolved source definition.
	sequenceDef.TableType = catalog.SystemSequenceRel
	ctx.tables[sequenceDef.Name] = sequenceDef
	ctx.objects[sequenceDef.Name] = &plan.ObjectRef{
		SchemaName: ctx.DefaultDatabase(),
		ObjName:    sequenceDef.Name,
	}

	for _, createSQL := range []string{
		"CREATE TABLE dst_live CLONE seq1",
		"CREATE TABLE dst_snapshot CLONE seq1 {SNAPSHOT = 'sp1'}",
		"CREATE TABLE dst_like LIKE seq1",
	} {
		t.Run(createSQL, func(t *testing.T) {
			createStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
			require.NoError(t, err)
			defer createStmt.Free()

			_, err = BuildPlan(ctx, createStmt, false)
			require.ErrorContains(t, err, "tpch.seq1 is not BASE TABLE")
		})
	}
}

func TestBuildPartitionedTablePersistsCanonicalSingleStatementSQL(t *testing.T) {
	const rootSQL = "/* before */ CREATE TABLE partitioned_t (category VARCHAR(20)) PARTITION BY LIST COLUMNS (category) (PARTITION p0 VALUES IN ('A'));"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	createTable := p.GetDdl().GetCreateTable()
	require.Equal(t, createTable.GetRawSQL(), tableDefCreateSQL(createTable.GetTableDef()))
	require.NotEqual(t, rootSQL, createTable.GetRawSQL())
}

func TestBuildCreateTablePersistsStatementCanonicalSQL(t *testing.T) {
	tests := []struct {
		name    string
		rootSQL string
		wantTmp []bool
	}{
		{
			name:    "temporary then permanent",
			rootSQL: "CREATE TEMPORARY TABLE temp_t(id int); CREATE TABLE permanent_t(id int)",
			wantTmp: []bool{true, false},
		},
		{
			name:    "permanent then temporary",
			rootSQL: "CREATE TABLE permanent_t(id int); CREATE TEMPORARY TABLE temp_t(id int)",
			wantTmp: []bool{false, true},
		},
		{
			name:    "comments between keywords",
			rootSQL: "CREATE /* first */ TEMPORARY -- second\n TABLE temp_t(id int); CREATE TABLE permanent_t(id int)",
			wantTmp: []bool{true, false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statements, err := parsers.Parse(context.Background(), dialect.MYSQL, test.rootSQL, 1)
			require.NoError(t, err)
			require.Len(t, statements, len(test.wantTmp))
			defer func() {
				for _, statement := range statements {
					statement.Free()
				}
			}()

			ctx := &rootSQLCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				rootSQL:             test.rootSQL,
			}
			for i, statement := range statements {
				createStmt := statement.(*tree.CreateTable)
				p, err := BuildPlan(ctx, createStmt, false)
				require.NoError(t, err)
				tableDef := p.GetDdl().GetCreateTable().GetTableDef()
				require.Equal(t, test.wantTmp[i], tableDef.GetTableType() == catalog.SystemTemporaryTable)
				require.False(t, tableDef.GetIsTemporary())
				require.Equal(t, canonicalCreateTableSQL(createStmt), tableDefCreateSQL(tableDef))
			}
		})
	}
}

func TestBuildTemporaryTableIndexDDLKeepsIndexRelkind(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		indexTables func(*plan.Plan) []*plan.TableDef
	}{
		{
			name: "create index",
			sql:  "create unique index uk_name on tpch.nation (n_name)",
			indexTables: func(p *plan.Plan) []*plan.TableDef {
				return p.GetDdl().GetCreateIndex().GetIndex().GetIndexTables()
			},
		},
		{
			name: "alter table add index",
			sql:  "alter table tpch.nation add unique index uk_name (n_name)",
			indexTables: func(p *plan.Plan) []*plan.TableDef {
				actions := p.GetDdl().GetAlterTable().GetActions()
				require.Len(t, actions, 1)
				return actions[0].GetAddIndex().GetIndexInfo().GetIndexTables()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			catalog.MarkTableDefTemporary(ctx.tables["nation"])
			// Resolve supplies this contextual bit for an existing temporary
			// table; the durable-marker helper intentionally does not.
			ctx.tables["nation"].IsTemporary = true
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			indexTables := test.indexTables(p)
			require.NotEmpty(t, indexTables)
			for _, tableDef := range indexTables {
				requireIndexCatalogRelkind(t, tableDef)
			}
		})
	}
}

func requireIndexCatalogRelkind(t *testing.T, tableDef *plan.TableDef) {
	t.Helper()
	require.NotEqual(t, catalog.SystemTemporaryTable, tableDef.TableType)
	require.False(t, tableDef.IsTemporary)

	kindCount := 0
	for _, def := range tableDef.Defs {
		for _, property := range def.GetProperties().GetProperties() {
			if property.Key == catalog.SystemRelAttr_Kind {
				kindCount++
				require.Equal(t, catalog.SystemIndexRel, property.Value)
			}
		}
	}
	require.Equal(t, 1, kindCount)
}

func requireTemporaryCatalogRelkind(t *testing.T, tableDef *plan.TableDef) {
	t.Helper()
	require.Equal(t, catalog.SystemTemporaryTable, tableDef.TableType)
	// IsTemporary is populated only when a session alias is resolved. CREATE
	// persists the TableType/relkind marker without manufacturing session state.
	require.False(t, tableDef.IsTemporary)

	kindCount := 0
	for _, def := range tableDef.Defs {
		for _, property := range def.GetProperties().GetProperties() {
			if property.Key == catalog.SystemRelAttr_Kind {
				kindCount++
				require.Equal(t, catalog.SystemTemporaryTable, property.Value)
			}
		}
	}
	require.Equal(t, 1, kindCount)
}

func TestBuildAlterView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	sql1 := "alter view v as select a from a"
	sql2 := "alter view v as select a from v"
	sql3 := "alter view v as select a from vx"

	store := make(map[string]arg)

	vData, err := json.Marshal(ViewData{
		Stmt:            "create view v as select a from a",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
	})
	assert.NoError(t, err)

	store["db.v"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vData),
			}},
	}

	vxData, err := json.Marshal(ViewData{
		Stmt:            "create view vx as select a from v",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
	})
	assert.NoError(t, err)
	store["db.vx"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vxData),
			}},
	}

	store["db.a"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "a",
					},
					Default: &plan.Default{
						Expr:         makePlan2StringConstExprWithType("seed"),
						OriginString: "'seed'",
					},
				},
			},
		}}

	store["db.verror"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel},
	}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().GetUserName().Return("sys:dump").AnyTimes()
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table, nil
		}).AnyTimes()
	ctx.EXPECT().SetBuildingAlterView(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetQueryingSubscription().Return(nil).AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().ResolveById(gomock.Any(), gomock.Any()).Return(nil, nil, nil).AnyTimes()
	ctx.EXPECT().GetStatsCache().Return(nil).AnyTimes()
	ctx.EXPECT().GetSnapshot().Return(nil).AnyTimes()
	ctx.EXPECT().SetViews(gomock.Any()).AnyTimes()
	ctx.EXPECT().SetSnapshot(gomock.Any()).AnyTimes()
	ctx.EXPECT().GetLowerCaseTableNames().Return(int64(1)).AnyTimes()
	ctx.EXPECT().GetSubscriptionMeta(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	alterPlan, err := buildAlterView(stmt1.(*tree.AlterView), ctx)
	assert.NoError(t, err)
	require.Equal(t, "'seed'", alterPlan.GetDdl().GetAlterView().GetTableDef().GetCols()[0].GetDefault().GetOriginString())
	require.Equal(t, "seed", alterPlan.GetDdl().GetAlterView().GetTableDef().GetCols()[0].GetDefault().GetExpr().GetLit().GetSval())
	require.Equal(t, ctx.GetAccountName(), "")

	//direct recursive refrence
	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt2.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	//indirect recursive refrence
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "vx").AnyTimes()
	assert.NoError(t, err)
	_, err = buildAlterView(stmt3.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	sql4 := "alter view noexists as select a from a"
	stmt4, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql4, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt4.(*tree.AlterView), ctx)
	assert.Error(t, err)

	sql5 := "alter view verror as select a from a"
	stmt5, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql5, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt5.(*tree.AlterView), ctx)
	assert.Error(t, err)
}

func TestBuildLockTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	store := make(map[string]arg)

	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read, t2 write"
	sql3 := "lock tables t1 read, t1 write"

	store["db.t1"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t1",
					},
				},
			},
		}}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table, nil
		}).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt1.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)

	store["db.t2"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t2",
					},
				},
			},
		}}

	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql3).AnyTimes()
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt3.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)
}

func TestBuildCreateTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))
	sqls := []string{
		`CREATE TABLE t3(
					col1 INT NOT NULL,
					col2 DATE NOT NULL UNIQUE KEY,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					PRIMARY KEY (col1),
					KEY(col3),
					KEY(col3) )`,
		`CREATE TABLE t2 (
						col1 INT NOT NULL,
						col2 DATE NOT NULL,
						col3 INT NOT NULL,
						col4 INT NOT NULL,
						UNIQUE KEY (col1),
						UNIQUE KEY (col3)
					);`,
		`CREATE TABLE t2 (
						col1 INT NOT NULL,
						col2 DATE NOT NULL,
						col3 INT NOT NULL,
						col4 INT NOT NULL,
						UNIQUE KEY (col1),
						UNIQUE KEY (col1, col3)
					);`,
		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					UNIQUE KEY (col1),
					UNIQUE KEY (col1, col3)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					UNIQUE KEY (col1),
					UNIQUE KEY (col1, col3)
				);`,

		`CREATE TABLE set_auto_increment (
			id SET('one', 'two') AUTO_INCREMENT
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1 DESC)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1 ASC)
		);`,

		"CREATE TABLE t2 (" +
			"	`PRIMARY` INT NOT NULL, " +
			"	col2 DATE NOT NULL, " +
			"	col3 INT NOT NULL," +
			"	col4 INT NOT NULL," +
			"	UNIQUE KEY (`PRIMARY`)," +
			"	UNIQUE KEY (`PRIMARY`, col3)" +
			");",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildCreateTableIdentifierLength(t *testing.T) {
	mock := NewMockOptimizer(false)
	validName := "表" + strings.Repeat("a", MaxIdentifierLength-1)
	plan, err := runOneStmt(mock, t, fmt.Sprintf("create table `%s` (id int)", validName))
	require.NoError(t, err)
	require.Equal(t, validName, plan.GetDdl().GetCreateTable().GetTableDef().GetName())

	for _, invalidName := range []string{
		"表" + strings.Repeat("b", MaxIdentifierLength),
		"表" + strings.Repeat("c", MaxIdentifierLength+1),
	} {
		_, err = runOneStmt(mock, t, fmt.Sprintf("create table `%s` (id int)", invalidName))
		require.Error(t, err)
		moErr, ok := err.(*moerr.Error)
		require.True(t, ok, "unexpected error type %T: %v", err, err)
		require.Equal(t, moerr.ErrTooLongIdent, moErr.ErrorCode())
		require.Equal(t, uint16(moerr.ER_TOO_LONG_IDENT), moErr.MySQLCode())
		require.Equal(t, fmt.Sprintf("Identifier name '%s' is too long", invalidName), moErr.Error())
	}

	internalMock := NewMockOptimizer(false)
	internalMock.ctxt.SetContext(context.WithValue(
		internalMock.ctxt.GetContext(),
		defines.InternalExecutorKey{},
		true,
	))
	internalName := "表" + strings.Repeat("i", MaxIdentifierLength)
	plan, err = runOneStmt(internalMock, t, fmt.Sprintf("create table `%s` (id int)", internalName))
	require.NoError(t, err)
	require.Equal(t, internalName, plan.GetDdl().GetCreateTable().GetTableDef().GetName())

	tempMock := NewMockOptimizer(false)
	tempCtx := &rootSQLCompilerContext{
		MockCompilerContext: &tempMock.ctxt,
		rootSQL:             "delete from temp_table",
	}
	physicalTempName := defines.GenTempTableName(
		tempCtx.GetProcess().GetSessionInfo().SessionId,
		"database",
		strings.Repeat("t", MaxIdentifierLength),
	)
	createTempSQL := fmt.Sprintf("create table `%s` (id int)", physicalTempName)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		createTempSQL,
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	plan, err = BuildPlan(tempCtx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, physicalTempName, plan.GetDdl().GetCreateTable().GetTableDef().GetName())

	tempCtx.rootSQL = createTempSQL
	_, err = BuildPlan(tempCtx, stmt, false)
	require.Error(t, err)
	moErr, ok := err.(*moerr.Error)
	require.True(t, ok, "unexpected error type %T: %v", err, err)
	require.Equal(t, moerr.ErrTooLongIdent, moErr.ErrorCode())
}

func TestBuildCatalogIdentifierLength(t *testing.T) {
	validName := "表" + strings.Repeat("a", MaxIdentifierLength-1)
	invalidName := "表" + strings.Repeat("b", MaxIdentifierLength)
	testCases := []struct {
		name string
		sql  func(string) string
	}{
		{
			name: "database",
			sql: func(name string) string {
				return fmt.Sprintf("create database `%s`", name)
			},
		},
		{
			name: "view",
			sql: func(name string) string {
				return fmt.Sprintf("create view `%s` as select 1", name)
			},
		},
		{
			name: "view column",
			sql: func(name string) string {
				return fmt.Sprintf("create view v as select 1 as `%s`", name)
			},
		},
		{
			name: "table column",
			sql: func(name string) string {
				return fmt.Sprintf("create table t (`%s` int)", name)
			},
		},
		{
			name: "table index",
			sql: func(name string) string {
				return fmt.Sprintf("create table t (a int, index `%s` (a))", name)
			},
		},
		{
			name: "table constraint",
			sql: func(name string) string {
				return fmt.Sprintf("create table t (a int, constraint `%s` unique (a))", name)
			},
		},
		{
			name: "create index",
			sql: func(name string) string {
				return fmt.Sprintf("create index `%s` on nation (n_nationkey)", name)
			},
		},
		{
			name: "alter add column",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation add column `%s` int", name)
			},
		},
		{
			name: "alter change column",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation change column n_name `%s` varchar(25)", name)
			},
		},
		{
			name: "alter rename column",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation rename column n_name to `%s`", name)
			},
		},
		{
			name: "alter add index",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation add index `%s` (n_nationkey)", name)
			},
		},
		{
			name: "alter add constraint",
			sql: func(name string) string {
				return fmt.Sprintf("alter table nation add constraint `%s` unique (n_nationkey)", name)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name+" accepts 64 characters", func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, testCase.sql(validName))
			require.NoError(t, err)
		})
		t.Run(testCase.name+" rejects 65 characters", func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, testCase.sql(invalidName))
			require.Error(t, err)
			moErr, ok := err.(*moerr.Error)
			require.True(t, ok, "unexpected error type %T: %v", err, err)
			require.Equal(t, moerr.ErrTooLongIdent, moErr.ErrorCode())
			require.Equal(t, uint16(moerr.ER_TOO_LONG_IDENT), moErr.MySQLCode())
		})
	}

	buildAlterView := func(t *testing.T, name string) error {
		mock := NewMockOptimizer(false)
		mock.ctxt.tables["v"] = &plan.TableDef{
			Name:    "v",
			ViewSql: &plan.ViewDef{View: `{"Stmt":"create view v as select 1","DefaultDatabase":"tpch"}`},
		}
		mock.ctxt.objects["v"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "v"}
		_, err := runOneStmt(mock, t, fmt.Sprintf("alter view v (`%s`) as select 1", name))
		return err
	}
	t.Run("alter view column accepts 64 characters", func(t *testing.T) {
		require.NoError(t, buildAlterView(t, validName))
	})
	t.Run("alter view column rejects 65 characters", func(t *testing.T) {
		err := buildAlterView(t, invalidName)
		require.Error(t, err)
		moErr, ok := err.(*moerr.Error)
		require.True(t, ok, "unexpected error type %T: %v", err, err)
		require.Equal(t, moerr.ErrTooLongIdent, moErr.ErrorCode())
		require.Equal(t, uint16(moerr.ER_TOO_LONG_IDENT), moErr.MySQLCode())
	})
}

func TestBuildCreateTableAcceptsTextBlobDisplayLength(t *testing.T) {
	tests := []struct {
		name    string
		typeSQL string
		wantID  types.T
	}{
		{name: "text", typeSQL: "text(4000)", wantID: types.T_text},
		{name: "blob", typeSQL: "blob(4000)", wantID: types.T_blob},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := runOneStmt(NewMockOptimizer(false), t,
				"create table display_length (value "+test.typeSQL+")")
			require.NoError(t, err)

			tableDef := plan.GetDdl().GetCreateTable().GetTableDef()
			var valueCol *ColDef
			for _, col := range tableDef.Cols {
				if col.Name == "value" {
					valueCol = col
					break
				}
			}
			require.NotNil(t, valueCol)
			require.Equal(t, int32(test.wantID), valueCol.Typ.Id)
		})
	}
}

func TestBuildCreateTableError(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqlerrs := []string{
		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL unique key,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key col2 (col3)
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key idx_sp1 (col2),
			unique key idx_sp1 (col3)
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key idx_sp1 (col2),
			key idx_sp1 (col3)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL UNIQUE KEY,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			KEY col2 (col3)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL KEY,
			col2 DATE NOT NULL KEY,
			col3 INT NOT NULL,
			col4 INT NOT NULL
		);`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY uk1 ((col1 + col3))
		);`,

		`CREATE TABLE enum_auto_increment (
			id ENUM('one', 'two') AUTO_INCREMENT
		);`,
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildAlterTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"ALTER TABLE emp ADD UNIQUE idx1 (empno, ename);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 (empno, ename);",
		"ALTER TABLE emp ADD INDEX idx1 (ename, sal);",
		"ALTER TABLE emp ADD INDEX idx2 (ename, sal DESC);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 (empno ASC);",
		//"alter table emp drop foreign key fk1",
		//"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(n_nationkey)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildCreateIndexOnExternalTableError(t *testing.T) {
	mock := NewEmptyMockOptimizer()
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.objects["ext_idx"] = &plan.ObjectRef{
		SchemaName: "tpch",
		ObjName:    "ext_idx",
	}
	ctx.tables["ext_idx"] = &plan.TableDef{
		Name:      "ext_idx",
		TableType: catalog.SystemExternalRel,
		Cols: []*plan.ColDef{
			{Name: "col_int32", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "col_varchar", Typ: plan.Type{Id: int32(types.T_varchar), Width: 100}},
			{Name: "part_id", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
	}

	sqls := []string{
		"CREATE INDEX idx_ext ON ext_idx(col_int32);",
		"CREATE UNIQUE INDEX uidx_ext ON ext_idx(col_int32);",
		"CREATE FULLTEXT INDEX fidx_ext ON ext_idx(col_varchar);",
		"ALTER TABLE ext_idx ADD INDEX idx_ext2 (col_int32);",
		"ALTER TABLE ext_idx ADD UNIQUE (col_varchar);",
		"ALTER TABLE ext_idx ADD FULLTEXT INDEX fidx_ext2 (col_varchar);",
	}
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "cannot create index on external table", sql)
	}
}

func TestBuildAlterTableRejectsMongoDBExternalTable(t *testing.T) {
	mock := NewEmptyMockOptimizer()
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.objects["mongo_ext"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "mongo_ext"}
	ctx.tables["mongo_ext"] = &plan.TableDef{
		Name:        "mongo_ext",
		TableType:   catalog.SystemExternalRel,
		FeatureFlag: features.MongoDBExternal,
		Cols: []*plan.ColDef{
			{Name: "device_id", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
			{Name: "measurement", Typ: plan.Type{Id: int32(types.T_float64)}},
		},
		Createsql: sqlmongodb.BuildCreateSQLEnvelope(sqlmongodb.TableMapping{
			Connection: "source", Database: "telemetry", Collection: "samples",
			SchemaMode: sqlmongodb.SchemaExplicit, Conversion: sqlmongodb.ConversionStrict,
			MaxParallelism: 1,
			Columns: []sqlmongodb.ColumnMapping{
				{Name: "device_id", Path: "metadata.device_id", TypeID: int32(types.T_varchar), Width: 64},
				{Name: "measurement", Path: "reading.measurement", TypeID: int32(types.T_float64)},
			},
		}),
	}

	for _, sql := range []string{
		"ALTER TABLE mongo_ext RENAME COLUMN device_id TO device_key",
		"ALTER TABLE mongo_ext MODIFY COLUMN measurement DECIMAL(18, 6)",
		"ALTER TABLE mongo_ext ADD COLUMN site_id VARCHAR(32)",
		"ALTER TABLE mongo_ext DROP COLUMN measurement",
	} {
		_, err := runOneStmt(mock, t, sql)
		require.ErrorContains(t, err, "ALTER TABLE on a MongoDB external table", sql)
	}
}

func TestBuildMongoDBExternalTableRejectsCheckConstraints(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	for _, sql := range []string{
		`CREATE EXTERNAL TABLE tpch.mongo_check (
			v BIGINT CHECK (v > 0)
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`,
		`CREATE EXTERNAL TABLE tpch.mongo_check (
			v BIGINT,
			CHECK (v > 0)
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`,
	} {
		_, err := runOneStmt(mock, t, sql)
		require.ErrorContains(t, err, "CHECK constraints on external tables", sql)
	}
}

func TestBuildMongoDBExternalTableRejectsGeneratedColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	_, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_generated (
			id VARCHAR(8) MONGODB_PATH '_id',
			x INT GENERATED ALWAYS AS (1) STORED
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples',
			"schema_mode"='explicit'
		)`)
	require.ErrorContains(t, err, "MongoDB external table does not support generated column 'x'")
}

func TestBuildMongoDBExternalTableRejectsOnUpdate(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	logicPlan, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_on_update (
			id VARCHAR(8) MONGODB_PATH '_id',
			ts DATETIME(3) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP MONGODB_PATH 'ts'
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`)
	require.Nil(t, logicPlan)
	require.ErrorContains(t, err, "MongoDB external table column 'ts' does not support ON UPDATE")
}

func TestBuildMongoDBExternalTableRejectsForeignKeys(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	logicPlan, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_fk (
			n_nationkey INT MONGODB_PATH '_id',
			CONSTRAINT fk_mongo_nation FOREIGN KEY (n_nationkey)
				REFERENCES tpch.nation (n_nationkey)
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`)
	require.Nil(t, logicPlan)
	require.ErrorContains(t, err, "FOREIGN KEY constraints on MongoDB external tables")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err.Error())
}

func TestBuildMongoDBExternalTableRejectsAutoIncrementBeforeCatalogDDL(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	logicPlan, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_auto_increment (
			id BIGINT AUTO_INCREMENT MONGODB_PATH '_id'
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`)
	require.ErrorContains(t, err, "MongoDB external table does not support AUTO_INCREMENT column 'id'")
	require.Nil(t, logicPlan, "validation must fail before a catalog DDL plan can be emitted")
}

func TestBuildMongoDBExternalTablePreservesNotNullMapping(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	logicPlan, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_not_null (
			v BIGINT NOT NULL MONGODB_PATH 'payload.value' MONGODB_CONVERT 'try_null'
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples'
		)`)
	require.NoError(t, err)
	tableDef := logicPlan.GetDdl().GetCreateTable().GetTableDef()
	require.NotEmpty(t, tableDef.Cols)
	require.True(t, features.IsMongoDBExternal(tableDef.FeatureFlag))
	require.Equal(t, "v", tableDef.Cols[0].Name)
	require.False(t, tableDef.Cols[0].Default.NullAbility)

	var createSQL string
	for _, def := range tableDef.Defs {
		for _, property := range def.GetProperties().GetProperties() {
			if property.Key == catalog.SystemRelAttr_CreateSQL {
				createSQL = property.Value
			}
		}
	}
	require.NotEmpty(t, createSQL)
	envelope, found, err := sqlmongodb.ParseCreateSQLEnvelope(t.Context(), createSQL)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, envelope.Columns, 1)
	require.True(t, envelope.Columns[0].NotNullable)
	require.True(t, sqlmongodb.ColumnsToPlan(envelope.Columns)[0].MoType.NotNullable)
}

func TestBuildMongoDBExternalTableRejectsSetColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	for _, members := range []struct {
		name string
		sql  string
	}{
		{name: "nonempty", sql: "'a','b'"},
		{name: "single_empty", sql: "''"},
	} {
		for _, nullability := range []struct {
			name string
			sql  string
		}{
			{name: "nullable"},
			{name: "not_null", sql: "NOT NULL"},
		} {
			for _, conversion := range []string{sqlmongodb.ConversionStrict, sqlmongodb.ConversionTryNull} {
				t.Run(members.name+"/"+nullability.name+"/"+conversion, func(t *testing.T) {
					sql := fmt.Sprintf(`
					CREATE EXTERNAL TABLE tpch.mongo_set (
						v SET(%s) %s MONGODB_PATH 'device_id'
					) ENGINE=MONGODB WITH (
						"connection"='source', "database"='telemetry', "collection"='samples',
						"schema_mode"='explicit', "conversion_mode"='%s', "max_parallelism"='1'
					)`, members.sql, nullability.sql, conversion)

					logicPlan, err := runOneStmt(mock, t, sql)
					require.ErrorContains(t, err, "MongoDB mapping target type SET")
					require.Nil(t, logicPlan, "failed CREATE must not retain a DDL plan or catalog mapping")
				})
			}
		}
	}
}

func TestBuildMongoDBExternalTableAcceptsUnsignedBigInt(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.SetContext(context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{Enable: true}},
	}))

	logicPlan, err := runOneStmt(mock, t, `
		CREATE EXTERNAL TABLE tpch.mongo_unsigned (
			v BIGINT UNSIGNED MONGODB_PATH 'device_id'
		) ENGINE=MONGODB WITH (
			"connection"='source', "database"='telemetry', "collection"='samples',
			"schema_mode"='explicit', "conversion_mode"='strict', "max_parallelism"='1'
		)`)
	require.NoError(t, err)
	require.NotNil(t, logicPlan)
	require.Equal(t, int32(types.T_uint64), logicPlan.GetDdl().GetCreateTable().GetTableDef().Cols[0].Typ.Id)
}

func TestBuildCreateExternalTableInlineIndexError(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE EXTERNAL TABLE ext_inline_col_key (id INT KEY) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_col_unique (id INT UNIQUE) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_col_pk (id INT PRIMARY KEY) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_key (id INT, KEY (id)) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_unique (id INT, UNIQUE KEY uk_id (id)) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_fulltext (doc VARCHAR(100), FULLTEXT ft_doc (doc)) INFILE {'filepath'='data.txt', 'format'='csv'};",
	}
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "cannot create index on external table", sql)
	}
}

func TestBuildAlterTableError(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"ALTER TABLE emp ADD UNIQUE idx1 ((empno+1) DESC, ename);",
		"ALTER TABLE emp ADD INDEX idx2 (ename, (sal*30) DESC);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 ((empno+20), (sal*30));",
	}
	runTestShouldError(mock, t, sqls)
}

func TestBuildIndexAllowsEnumAndTextBlobPrefix(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE enum_idx_ok1 (id VARCHAR(191) PRIMARY KEY, role ENUM('a','b','c'), INDEX idx_role(role));",
		"CREATE TABLE enum_idx_ok2 (id VARCHAR(191) PRIMARY KEY, role ENUM('a','b','c'), UNIQUE INDEX uq_role(role));",
		"CREATE TABLE enum_idx_ok3 (id VARCHAR(191) PRIMARY KEY, name VARCHAR(191), role ENUM('a','b','c'), INDEX idx_name_role(name, role));",
		"CREATE TABLE text_prefix_ok1 (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t(100)));",
		"CREATE TABLE text_prefix_ok2 (id INT PRIMARY KEY, t TEXT, UNIQUE INDEX uq_t(t(100)));",
		"CREATE TABLE blob_prefix_ok1 (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b(100)));",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildIndexRejectsTextBlobPlainIndex(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqlerrs := []string{
		"CREATE TABLE text_plain_err1 (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t));",
		"CREATE TABLE text_plain_err2 (id INT PRIMARY KEY, t TEXT, UNIQUE INDEX uq_t(t));",
		"CREATE TABLE text_comp_pk_err (id INT, t TEXT, PRIMARY KEY(id, t));",
		"CREATE TABLE blob_plain_err1 (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b));",
		"CREATE TABLE blob_comp_pk_err (b BLOB, id INT, PRIMARY KEY(b, id));",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildRegularSecondaryIndexPersistsPrefixLengths(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name   string
		sql    string
		column string
		length int
	}{
		{
			name:   "text",
			sql:    "CREATE TABLE text_prefix_secondary_ok (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t(100)));",
			column: "t",
			length: 100,
		},
		{
			name:   "blob",
			sql:    "CREATE TABLE blob_prefix_secondary_ok (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b(100)));",
			column: "b",
			length: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)

			createTable := logicPlan.GetDdl().GetCreateTable()
			require.NotNil(t, createTable)
			require.Len(t, createTable.GetTableDef().GetIndexes(), 1)

			indexDef := createTable.GetTableDef().GetIndexes()[0]
			prefixLengths := catalog.IndexPrefixLengthsFromParams(indexDef.IndexAlgoParams)
			require.Equal(t, tt.length, prefixLengths[tt.column])
		})
	}
}

func TestBuildIndexPersistsVisibility(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name    string
		sql     string
		visible bool
	}{
		{
			name:    "default regular index is visible",
			sql:     "CREATE TABLE idx_visibility_default (id INT PRIMARY KEY, a INT, KEY idx_a(a))",
			visible: true,
		},
		{
			name:    "explicit visible regular index",
			sql:     "CREATE TABLE idx_visibility_visible (id INT PRIMARY KEY, a INT, KEY idx_a(a) VISIBLE)",
			visible: true,
		},
		{
			name:    "invisible regular index",
			sql:     "CREATE TABLE idx_visibility_invisible (id INT PRIMARY KEY, a INT, KEY idx_a(a) INVISIBLE)",
			visible: false,
		},
		{
			name:    "invisible unique index",
			sql:     "CREATE TABLE idx_visibility_unique (id INT PRIMARY KEY, a INT, UNIQUE KEY idx_a(a) INVISIBLE)",
			visible: false,
		},
		{
			name:    "invisible fulltext index",
			sql:     "CREATE TABLE idx_visibility_fulltext (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY idx_body(body) INVISIBLE)",
			visible: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			indexes := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetIndexes()
			require.NotEmpty(t, indexes)
			for _, indexDef := range indexes {
				got, isSet := catalog.GetIndexVisibility(indexDef)
				require.True(t, isSet)
				require.Equal(t, tc.visible, got)
				require.Equal(t, tc.visible, indexDef.Visible)
			}
		})
	}
}

func TestBuildPrefixIndexV2ProtocolGate(t *testing.T) {
	mock := NewMockOptimizer(false)
	proc := mock.CurrentContext().GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	_, err := runOneStmt(mock, t,
		"CREATE TABLE prefix_v1_ok (id INT PRIMARY KEY, name VARCHAR(32), INDEX idx_name(name(4)))")
	require.NoError(t, err)
	_, err = runOneStmt(mock, t,
		"CREATE TABLE prefix_v2_blocked (id INT PRIMARY KEY, `head:line` VARCHAR(32), INDEX idx_name(`head:line`(4)))")
	require.ErrorContains(t, err, "protocol version 13")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion13)
	logicPlan, err := runOneStmt(mock, t,
		"CREATE TABLE prefix_v2_ok (id INT PRIMARY KEY, `head:line` VARCHAR(32), INDEX idx_name(`head:line`(4)))")
	require.NoError(t, err)
	indexDef := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetIndexes()[0]
	require.Equal(t, map[string]int{"head:line": 4}, catalog.IndexPrefixLengthsFromParams(indexDef.IndexAlgoParams))
}

func TestBuildCompositeIndexMarksEncodedKeyBinary(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
		"create table composite_key_charset (id int primary key, a varchar(10), b varchar(10), index idx_ab(a, b))")
	require.NoError(t, err)

	createTable := logicPlan.GetDdl().GetCreateTable()
	require.NotNil(t, createTable)
	require.Len(t, createTable.IndexTables, 1)
	key := FindColumn(createTable.IndexTables[0].Cols, catalog.IndexTableIndexColName)
	require.NotNil(t, key)
	require.Equal(t, uint32(types.CharsetBinary), key.Typ.Charset)
}

func TestBuildVectorIndexAllowsIvfFlatOnly(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE vec_idx_ok1 (id INT PRIMARY KEY, embedding VECF32(3), KEY idx_emb USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops');",
		"CREATE TABLE vec_idx_ok2 (id INT PRIMARY KEY, embedding VECF64(3), KEY idx_emb USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops');",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	sqlerrs := []string{
		"CREATE TABLE vec_idx_err1 (id INT PRIMARY KEY, embedding VECF32(3), KEY idx_emb (embedding));",
		"CREATE TABLE vec_idx_err2 (id INT PRIMARY KEY, embedding VECF64(3), KEY idx_emb (embedding));",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildIndexAllowsRTreeGeometry(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE geo_spatial_ok (id INT PRIMARY KEY, g POINT NOT NULL, KEY idx_g USING RTREE (g));",
		"CREATE TABLE geo_spatial_nullable_ok (id INT PRIMARY KEY, g POINT, KEY idx_g USING RTREE (g));",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestGeometryDDLGuardsSQLPaths(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	sqlerrs := []string{
		"CREATE TABLE geo_default_err (g GEOMETRY DEFAULT 'POINT(1 1)');",
		"CREATE TABLE geo_pk_err (g GEOMETRY PRIMARY KEY);",
		"CREATE TABLE geo_uk_err (g GEOMETRY UNIQUE KEY);",
		"CREATE TABLE geo_idx_err (g GEOMETRY, KEY(g));",
		"ALTER TABLE emp ADD COLUMN g GEOMETRY UNIQUE KEY;",
		"ALTER TABLE emp ADD COLUMN g GEOMETRY PRIMARY KEY;",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestGeometryColumnValidationSQLPaths(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	sqls := []string{
		"CREATE TABLE geo_point_ok (g POINT);",
		"CREATE TABLE geo_any_ok (g GEOMETRY);",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestCreateSingleTable(t *testing.T) {
	sql := "create cluster table a (a int);"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestCreateTableAsSelect(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{"CREATE TABLE t1 (a int, b char(5)); CREATE TABLE t2 (c float) as select b, a from t1"}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildCTASAggregateNullabilityAndDefaults(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, `
		create table aggregate_metadata as
		select count(n_name) as cnt,
			bit_and(n_nationkey) as band,
			bit_or(n_nationkey) as bor,
			bit_xor(n_nationkey) as bxor,
			min(n_nationkey) as minimum,
			max(n_nationkey) as maximum
		from nation`)
	require.NoError(t, err)

	var visible []*plan.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 6)

	for _, idx := range []int{0, 1, 2, 3} {
		col := visible[idx]
		require.True(t, col.Typ.NotNullable, col.Name)
		require.NotNil(t, col.Default, col.Name)
		require.False(t, col.Default.NullAbility, col.Name)
		require.Equal(t, "0", col.Default.OriginString, col.Name)
		require.NotNil(t, col.Default.Expr, col.Name)
	}
	require.Equal(t, int32(types.T_int64), visible[0].Typ.Id)
	for _, idx := range []int{1, 2, 3} {
		require.Equal(t, int32(types.T_uint64), visible[idx].Typ.Id, visible[idx].Name)
	}

	for _, idx := range []int{4, 5} {
		col := visible[idx]
		require.False(t, col.Typ.NotNullable, col.Name)
		require.NotNil(t, col.Default, col.Name)
		require.True(t, col.Default.NullAbility, col.Name)
		require.Empty(t, col.Default.OriginString, col.Name)
		require.Nil(t, col.Default.Expr, col.Name)
	}
}

func TestBuildCTASHLLAggregatesHaveNoExecutableDefault(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, `
		create table hll_metadata as
		select hll_add_agg(n_nationkey) as added,
			hll_merge_agg(cast(n_name as varbinary)) as merged
		from nation`)
	require.NoError(t, err)

	var visible []*plan.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 2)
	for _, col := range visible {
		require.Equal(t, int32(types.T_varbinary), col.Typ.Id, col.Name)
		require.True(t, col.Typ.NotNullable, col.Name)
		require.NotNil(t, col.Default, col.Name)
		require.False(t, col.Default.NullAbility, col.Name)
		require.Empty(t, col.Default.OriginString, col.Name)
		require.Nil(t, col.Default.Expr, col.Name)
	}
}

func TestBuildCTASDoesNotCopyAutoIncrement(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctx := &mock.ctxt
	sourceCol := ctx.tables["nation"].Cols[0]
	sourceCol.Typ.AutoIncr = true
	sourceCol.Default = nil

	stmt, err := parsers.ParseOne(
		t.Context(),
		dialect.MYSQL,
		"create table copied as select n_nationkey as id, n_name as payload from nation",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	var visible []*plan.ColDef
	for _, col := range cols {
		if !col.GetHidden() {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 2)
	var idCol *plan.ColDef
	for _, col := range visible {
		if col.Name == "id" {
			idCol = col
			break
		}
	}
	require.NotNil(t, idCol)
	require.False(t, idCol.Typ.AutoIncr)
	require.NotNil(t, idCol.Default)
	require.False(t, idCol.Default.NullAbility)
	require.Equal(t, "0", idCol.Default.OriginString)
	require.NotNil(t, idCol.Default.Expr)
	require.Equal(t, int32(types.T_int32), idCol.Default.Expr.Typ.Id)
	require.Equal(t, int32(0), idCol.Default.Expr.GetLit().GetI32Val())

	target := p.GetDdl().GetCreateTable()
	tableDef := target.GetTableDef()
	tableDef.TblId = 99102
	ctx.objects[tableDef.Name] = &ObjectRef{SchemaName: "tpch", ObjName: tableDef.Name, Obj: int64(tableDef.TblId)}
	ctx.tables[tableDef.Name] = tableDef
	ctx.id2name[tableDef.TblId] = tableDef.Name
	ctx.pks[tableDef.Name] = nil
	_, err = runOneStmt(mock, t, "insert into copied(payload) values ('omitted-id')")
	require.NoError(t, err)
}

func TestBuildCTASExplicitTargetDefaultOverridesAutoIncrementTypeDefault(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	sourceCol := ctx.tables["nation"].Cols[0]
	sourceCol.Typ.AutoIncr = true
	sourceCol.Default = nil

	stmt, err := parsers.ParseOne(
		t.Context(),
		dialect.MYSQL,
		"create table copied_explicit (id int not null default 42, payload varchar(25)) as select n_nationkey as id, n_name as payload from nation",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	var idCol *plan.ColDef
	for _, col := range cols {
		if !col.Hidden && col.Name == "id" {
			idCol = col
			break
		}
	}
	require.NotNil(t, idCol)
	require.False(t, idCol.Typ.AutoIncr)
	require.Equal(t, "42", idCol.Default.OriginString)
	require.NotNil(t, idCol.Default.Expr)
	require.Equal(t, int32(42), idCol.Default.Expr.GetLit().GetI32Val())
}

func TestCreateTableAsSelectPropagatesNullExtension(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		nullAbility []bool
	}{
		{
			name: "inner join control",
			sql: "create table ctas_inner as select n.n_nationkey as left_key, r.r_regionkey as right_key " +
				"from nation n join region r on n.n_regionkey = r.r_regionkey",
			nullAbility: []bool{false, false},
		},
		{
			name: "left join null extends right",
			sql: "create table ctas_left as select n.n_nationkey as left_key, r.r_regionkey as right_key " +
				"from nation n left join region r on n.n_regionkey = r.r_regionkey",
			nullAbility: []bool{false, true},
		},
		{
			name: "right join null extends left",
			sql: "create table ctas_right as select n.n_nationkey as left_key, r.r_regionkey as right_key " +
				"from nation n right join region r on n.n_regionkey = r.r_regionkey",
			nullAbility: []bool{true, false},
		},
		{
			name: "full join null extends both sides",
			sql: "create table ctas_full as select n.n_nationkey as left_key, r.r_regionkey as right_key " +
				"from nation n full join region r on n.n_regionkey = r.r_regionkey",
			nullAbility: []bool{true, true},
		},
		{
			name: "correlated scalar subquery may not match",
			sql: "create table ctas_scalar as select n.n_nationkey as left_key, " +
				"(select r.r_regionkey from region r where r.r_regionkey = n.n_regionkey) as scalar_key from nation n",
			nullAbility: []bool{false, true},
		},
		{
			name: "coalesce control removes null extension",
			sql: "create table ctas_coalesce as select n.n_nationkey as left_key, " +
				"coalesce(r.r_regionkey, 0) as right_key from nation n left join region r on n.n_regionkey = r.r_regionkey",
			nullAbility: []bool{false, false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			require.True(t, mock.ctxt.tables["nation"].Cols[0].Typ.NotNullable)
			require.True(t, mock.ctxt.tables["region"].Cols[0].Typ.NotNullable)

			logicPlan, err := buildSingleStmt(mock, t, test.sql)
			require.NoError(t, err)
			var visibleCols []*plan.ColDef
			for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
				if !col.Hidden {
					visibleCols = append(visibleCols, col)
				}
			}
			require.Len(t, visibleCols, len(test.nullAbility))
			for i, want := range test.nullAbility {
				require.Equal(t, want, visibleCols[i].GetDefault().GetNullAbility())
			}
		})
	}
}

func TestDynamicStringIntervalCanProduceNullInCTASAndView(t *testing.T) {
	const selectSQL = "select " +
		"date_add(cast('2026-01-01' as date), interval n_name year_month) as add_result, " +
		"date_sub(cast('2026-01-01' as date), interval n_name year_month) as sub_result " +
		"from nation"

	t.Run("CTAS", func(t *testing.T) {
		mock := NewMockOptimizer(false)
		logicPlan, err := buildSingleStmt(mock, t, "create table ctas_interval as "+selectSQL)
		require.NoError(t, err)

		for _, column := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()[:2] {
			require.True(t, column.GetDefault().GetNullAbility(),
				"invalid values in NOT NULL n_name can make to_interval and date arithmetic return NULL")
		}
	})

	t.Run("view", func(t *testing.T) {
		ctx := NewMockCompilerContext(false)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create view interval_view as "+selectSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()

		logicPlan, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)

		for _, column := range logicPlan.GetDdl().GetCreateView().GetTableDef().GetCols()[:2] {
			require.True(t, column.GetDefault().GetNullAbility(),
				"view metadata must preserve date arithmetic nullability from dynamic interval normalization")
		}
	})
}

func TestCreateTableAsSelectPreservesSpecialTypeNullability(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table copied as select n.priority from nation n right join region r on n.n_regionkey = r.r_regionkey", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	col := p.GetDdl().GetCreateTable().GetTableDef().GetCols()[0]
	require.True(t, isEnumPlanType(&col.Typ))
	require.Equal(t, "low,medium,high", col.Typ.GetEnumvalues())
	require.False(t, col.Typ.GetNotNullable())
	require.True(t, col.GetDefault().GetNullAbility())
}

func TestCreateTableAsSelectWithTemporalFractionalSeconds(t *testing.T) {
	tests := []struct {
		name       string
		literal    string
		castType   string
		oid        types.T
		precision  int32
		columnName string
	}{
		{name: "time", literal: "07:08:09.123456", castType: "time(3)", oid: types.T_time, precision: 3, columnName: "time_lit"},
		{name: "datetime", literal: "2025-05-06 07:08:09.123456", castType: "datetime(6)", oid: types.T_datetime, precision: 6, columnName: "datetime_lit"},
		{name: "timestamp", literal: "2025-05-06 07:08:09.123456", castType: "timestamp(6)", oid: types.T_timestamp, precision: 6, columnName: "timestamp_lit"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			sql := "create table ctas_" + test.name + " as select cast('" + test.literal + "' as " + test.castType + ") as " + test.columnName
			plan, err := buildSingleStmt(mock, t, sql)
			require.NoError(t, err)

			createTable := plan.GetDdl().GetCreateTable()
			require.NotEmpty(t, createTable.TableDef.Cols)
			column := createTable.TableDef.Cols[0]
			require.Equal(t, test.columnName, column.Name)
			require.Equal(t, int32(test.oid), column.Typ.Id)
			require.Equal(t, test.precision, column.Typ.Width)
			require.Equal(t, test.precision, column.Typ.Scale)
			if test.oid == types.T_datetime {
				require.True(t, column.Default.NullAbility)
			}

			createAsSelect := createTable.GetCreateAsSelectSql()
			require.Contains(t, createAsSelect, " as "+test.castType+")")
			require.NotContains(t, createAsSelect, test.castType[:len(test.castType)-1]+",")
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, createAsSelect, 1)
			require.NoError(t, err)
			stmt.Free()
		})
	}
}

func TestCreateTableAsSelectWithTimestampPairPrecision(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		wantFSP    int32
	}{
		{name: "string literals fsp zero", expression: "timestamp('2024-01-15', '12:30:00')", wantFSP: 0},
		{name: "string literals fsp one", expression: "timestamp('2024-01-15 10:00:00.1', '02:30:00')", wantFSP: 1},
		{name: "second datetime literal fsp one", expression: "timestamp('2024-01-15', '2024-01-15 12:30:00.1')", wantFSP: 1},
		{name: "string literals fsp six", expression: "timestamp('2024-01-15', '12:30:00.123456')", wantFSP: 6},
		{name: "typed values fsp six", expression: "timestamp(cast('2024-01-15' as date), cast('12:30:00.123456' as time(6)))", wantFSP: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := buildSingleStmt(mock, t,
				"create table timestamp_pair_ctas as select "+test.expression+" as pair_value")
			require.NoError(t, err)

			column := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()[0]
			require.Equal(t, int32(types.T_datetime), column.Typ.Id)
			require.Equal(t, test.wantFSP, column.Typ.Width)
			require.Equal(t, test.wantFSP, column.Typ.Scale)
			require.True(t, column.GetDefault().GetNullAbility())
		})
	}
}

func TestCreateTableAsSelectPreservesTimeWindowMicrosecondBoundaryScale(t *testing.T) {
	mock := NewMockOptimizer(false)
	mockTimeWindowScaleTable(t, mock, types.T_datetime.ToTypeWithScale(0))

	logicPlan, err := buildSingleStmt(mock, t,
		"create table hf_scale_materialized as "+
			"select _wstart, _wend, count(*) as row_count "+
			"from tw_scale interval(ts, 1, microsecond)")
	require.NoError(t, err)

	createTable := logicPlan.GetDdl().GetCreateTable()
	require.NotNil(t, createTable)
	require.GreaterOrEqual(t, len(createTable.TableDef.Cols), 2)
	for _, col := range createTable.TableDef.Cols[:2] {
		require.Equal(t, int32(types.T_datetime), col.Typ.Id, col.Name)
		require.Equal(t, int32(6), col.Typ.Scale, col.Name)
		require.Equal(t, int32(6), col.Typ.Width, col.Name)
	}
}

func TestCreateTableAsSelectTimeWindowBoundaryType(t *testing.T) {
	tests := []struct {
		name     string
		castType string
		oid      types.T
		scale    int32
	}{
		{name: "date", castType: "date", oid: types.T_datetime, scale: 0},
		{name: "datetime scale zero", castType: "datetime", oid: types.T_datetime, scale: 0},
		{name: "datetime scale six", castType: "datetime(6)", oid: types.T_datetime, scale: 6},
		{name: "timestamp scale three", castType: "timestamp(3)", oid: types.T_timestamp, scale: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			sql := "create table tw_rollup as " +
				"select _wstart as ws, _wend as we, count(*) as c " +
				"from (select 1 as k, cast('2026-01-01 00:00:01.123456' as " + test.castType + ") as event_ts) src " +
				"group by k interval(event_ts, 1, minute)"
			plan, err := buildSingleStmt(mock, t, sql)
			require.NoError(t, err)

			cols := plan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.GreaterOrEqual(t, len(cols), 3)
			for _, idx := range []int{0, 1} {
				require.Equal(t, int32(test.oid), cols[idx].Typ.Id, cols[idx].Name)
				require.Equal(t, test.scale, cols[idx].Typ.Scale, cols[idx].Name)
				require.False(t, cols[idx].Default.NullAbility, cols[idx].Name)
			}
		})
	}
}

func TestCreateTableAsSelectKeepsNonTemporalLiteralNotNull(t *testing.T) {
	mock := NewMockOptimizer(false)
	plan, err := buildSingleStmt(mock, t, "create table ctas_literal as select 1 as n")
	require.NoError(t, err)

	column := plan.GetDdl().GetCreateTable().TableDef.Cols[0]
	require.False(t, column.Default.NullAbility)
}

func TestCreateTableAsSelectTemporalInsertKeepsTargetScale(t *testing.T) {
	mock := NewMockOptimizer(false)
	ctas, err := buildSingleStmt(mock, t, "create table ctas_datetime6 as select cast('2025-05-06 07:08:09.123456' as datetime(6)) as dt")
	require.NoError(t, err)

	createTable := ctas.GetDdl().GetCreateTable()
	tableDef := createTable.GetTableDef()
	tableDef.TblId = 99101
	mock.ctxt.objects[tableDef.Name] = &ObjectRef{SchemaName: "tpch", ObjName: tableDef.Name, Obj: int64(tableDef.TblId)}
	mock.ctxt.tables[tableDef.Name] = tableDef
	mock.ctxt.id2name[tableDef.TblId] = tableDef.Name
	mock.ctxt.pks[tableDef.Name] = nil

	insertPlan, err := runOneStmt(mock, t, createTable.GetCreateAsSelectSql())
	require.NoError(t, err)

	var found bool
	for _, node := range insertPlan.GetQuery().GetNodes() {
		for _, expr := range node.GetProjectList() {
			if types.T(expr.GetTyp().Id) == types.T_datetime {
				found = true
				require.Equal(t, int32(6), expr.GetTyp().Scale)
			}
		}
	}
	require.True(t, found)
}

func TestPrepareCreateTableAsSelectWithParams(t *testing.T) {
	mock := NewMockOptimizer(false)

	prepared, err := runOneStmt(mock, t, "prepare stmt_ctas from 'create table ctas_p as select ? as a, ? as b'")
	require.NoError(t, err)
	prepare := prepared.GetDcl().GetPrepare()
	require.Len(t, prepare.GetParamTypes(), 2)
	require.NotNil(t, prepare.GetPlan().GetDdl().GetQuery())
	require.Empty(t, GetResultColumnsFromPlan(prepare.GetPlan()))

	prepared, err = runOneStmt(mock, t, "prepare stmt_ctas_where from 'create table ctas_where as select N_NAME from NATION where N_REGIONKEY = ?'")
	require.NoError(t, err)
	prepare = prepared.GetDcl().GetPrepare()
	require.Len(t, prepare.GetParamTypes(), 1)
	require.NotEmpty(t, prepare.GetSchemas())
	require.False(t, prepare.GetPlan().GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetDefault().GetNullAbility())

	prepared, err = runOneStmt(mock, t, "prepare stmt_ctas_join from 'create table ctas_join as select n.N_NATIONKEY, r.R_REGIONKEY from NATION n left join REGION r on n.N_REGIONKEY = r.R_REGIONKEY where n.N_NATIONKEY = ?'")
	require.NoError(t, err)
	prepare = prepared.GetDcl().GetPrepare()
	createTable := prepare.GetPlan().GetDdl().GetCreateTable()
	require.NotNil(t, prepare.GetPlan().GetDdl().GetQuery())
	require.False(t, createTable.GetTableDef().GetCols()[0].GetDefault().GetNullAbility())
	require.True(t, createTable.GetTableDef().GetCols()[1].GetDefault().GetNullAbility())

	_, err = runOneStmt(mock, t, "create table ctas_unprepared as select ? as a")
	require.ErrorContains(t, err, "only prepare statement can use ? expr")
}

func TestCreateTableAsSelectQuotesIdentifiers(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "non-ASCII select alias",
			sql:  "CREATE TABLE ctas_alias AS SELECT N_NAME AS `中文别名` FROM NATION",
			want: "insert into `tpch`.`ctas_alias` select * from (select `nation`.`N_NAME` as `中文别名` from `nation`) as __mo_ctas_source",
		},
		{
			name: "reserved table alias",
			sql:  "CREATE TABLE ctas_alias AS SELECT `order`.N_NAME AS `select` FROM NATION AS `order`",
			want: "insert into `tpch`.`ctas_alias` select * from (select `order`.`N_NAME` as `select` from `nation` as `order`) as __mo_ctas_source",
		},
		{
			name: "embedded backtick in target name",
			sql:  "CREATE TABLE `ctas``alias` AS SELECT N_NAME FROM NATION",
			want: "insert into `tpch`.`ctas``alias` select * from (select `nation`.`N_NAME` from `nation`) as __mo_ctas_source",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(mock, t, test.sql)
			require.NoError(t, err)

			createTable := logicPlan.GetDdl().GetCreateTable()
			require.NotNil(t, createTable)
			require.Equal(t, test.want, createTable.GetCreateAsSelectSql())
		})
	}
}

func TestCreateTableAsSelectPreservesGroupConcatOrderBy(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(
		mock,
		t,
		"create table ctas_group_concat as select N_REGIONKEY, group_concat(N_NAME order by N_NAME) as names from NATION group by N_REGIONKEY",
	)
	require.NoError(t, err)

	createTable := logicPlan.GetDdl().GetCreateTable()
	require.NotNil(t, createTable)
	require.Contains(
		t,
		createTable.GetCreateAsSelectSql(),
		"group_concat(`nation`.`N_NAME` order by `N_NAME` separator \",\")",
	)
}

func TestCreateTableAsSelectPreservesIntervalSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "interval expressions",
			sql:  "select date_add(col2, interval(45, day)), date_sub(col2, interval(5, day)) from time01",
			want: "select date_add(col2, interval 45 day), date_sub(col2, interval 5 day) from time01",
		},
		{
			name: "interval text in identifier",
			sql:  "select `interval(x,day)` from src as `interval(y,month)`",
			want: "select `interval(x,day)` from src as `interval(y,month)`",
		},
		{
			name: "doubled backtick in identifier",
			sql:  "select `a``interval(x,day)` from src",
			want: "select `a``interval(x,day)` from src",
		},
		{
			name: "unclosed backtick",
			sql:  "select `interval(x,day)",
			want: "select `interval(x,day)",
		},
		{
			name: "quoted interval operand",
			sql:  "select date_add(col2, interval(`a,b)`, day)) from src",
			want: "select date_add(col2, interval `a,b)` day) from src",
		},
		{
			name: "single quoted string",
			sql:  "select 'interval(1,day)' as c",
			want: "select 'interval(1,day)' as c",
		},
		{
			name: "double quoted string",
			sql:  `select "interval(1,day)" as c`,
			want: `select "interval(1,day)" as c`,
		},
		{
			name: "doubled quote in string",
			sql:  "select 'a''interval(1,day)' as c",
			want: "select 'a''interval(1,day)' as c",
		},
		{
			name: "backslash escaped quote in string",
			sql:  `select 'a\'interval(1,day)' as c`,
			want: `select 'a\'interval(1,day)' as c`,
		},
		{
			name: "unclosed quoted string",
			sql:  "select 'interval(1,day)",
			want: "select 'interval(1,day)",
		},
		{
			name: "identifier prefix",
			sql:  "select myinterval(1, day), $interval(2, day), 中文interval(3, day)",
			want: "select myinterval(1, day), $interval(2, day), 中文interval(3, day)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, restoreIntervalSyntaxForCTAS(test.sql))
		})
	}
}

func TestParseDuration(t *testing.T) {

	cases := []struct {
		period      uint64
		unit        string
		expected    time.Duration
		expectedErr error
	}{
		// nil input
		{expectedErr: moerr.NewInvalidArg(context.Background(), "time unit", "")},
		// 0 second
		{0, "second", 0, nil},
		// 1 second
		{1, "second", time.Second, nil},
		// 2 minute
		{2, "minute", 2 * time.Minute, nil},
		// 3 hour
		{3, "hour", 3 * time.Hour, nil},
		// 4 day
		{4, "day", 4 * 24 * time.Hour, nil},
		// 5 week
		{5, "week", 5 * 7 * 24 * time.Hour, nil},
		// 6 month
		{6, "month", 6 * 30 * 24 * time.Hour, nil},
		// invalid time unit: year
		{7, "year", 0, moerr.NewInvalidArg(context.Background(), "time unit", "year")},
	}

	for _, c := range cases {
		duration, err := parseDuration(context.Background(), c.period, c.unit)
		assert.Equal(t, c.expected, duration)
		assert.Equal(t, err, c.expectedErr)
	}
}

func Test_buildTableDefs(t *testing.T) {
	stmt := &tree.CreateTable{
		Temporary:          false,
		IsClusterTable:     false,
		IfNotExists:        false,
		Table:              tree.TableName{},
		Defs:               nil,
		Options:            nil,
		PartitionOption:    nil,
		ClusterByOption:    nil,
		Param:              nil,
		AsSource:           &tree.Select{Select: &tree.SelectClause{From: &tree.From{}}},
		IsAsSelect:         true,
		IsAsLike:           false,
		LikeTableName:      tree.TableName{},
		SubscriptionOption: nil,
	}

	ctx := &MockCompilerContext{}

	createTable := &plan.CreateTable{
		Database: "db",
		TableDef: &plan.TableDef{
			Name: "table",
		},
	}

	err := buildTableDefs(stmt, ctx, createTable, nil)
	assert.Error(t, err)
}

func TestBuildCreatePitr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Helper to create a base stmt
	baseStmt := func() *tree.CreatePitr {
		return &tree.CreatePitr{
			IfNotExists: true,
			Name:        "pitr1",
			Level:       tree.PITRLEVELCLUSTER,
			PitrValue:   1,
			PitrUnit:    "h",
		}
	}

	t.Run("sys account can create cluster level pitr", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		require.Equal(t, ctx.GetAccountName(), "sys")
	})

	t.Run("non-sys account cannot create cluster level pitr", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "user1" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 2, nil }
		stmt := baseStmt()
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only sys tenant can create cluster level pitr")
	})

	t.Run("sys account can create account level pitr for self", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.ResolveAccountIdsFunc = func(_ []string) ([]uint32, error) { return []uint32{1}, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELACCOUNT
		stmt.AccountName = "sys"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
	})

	t.Run("non-sys account cannot create account level pitr for other", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "user1" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 2, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELACCOUNT
		stmt.AccountName = "other"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only sys tenant can create tenant level pitr for other tenant")
	})

	t.Run("invalid pitr value", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.PitrValue = 0
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pitr value")
	})

	t.Run("invalid pitr unit", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.PitrUnit = "invalid"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pitr unit")
	})

	t.Run("reserved pitr name", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.Name = "sys_mo_catalog_pitr"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pitr name is reserved")
	})

	t.Run("database level pitr, database not exist", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return false }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELDATABASE
		stmt.DatabaseName = "db1"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database db1 does not exist")
	})

	t.Run("database level pitr, database exists", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.GetDatabaseIdFunc = func(string, *Snapshot) (uint64, error) { return 123, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELDATABASE
		stmt.DatabaseName = "db1"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
	})

	t.Run("table level pitr, table not exist", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.ResolveFunc = func(string, string, *Snapshot) (*ObjectRef, *TableDef) { return nil, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELTABLE
		stmt.DatabaseName = "db1"
		stmt.TableName = "tb1"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table db1.tb1 does not exist")
	})

	t.Run("table level pitr, table exists", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.ResolveFunc = func(string, string, *Snapshot) (*ObjectRef, *TableDef) { return &ObjectRef{}, &TableDef{TblId: 456} }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELTABLE
		stmt.DatabaseName = "db1"
		stmt.TableName = "tb1"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Nil(t, plan)
	})
}

func TestConstructAddedPartitionDefsErrors(t *testing.T) {
	ctx := NewEmptyCompilerContext()
	ctx.SetContext(context.Background())

	makeTableDef := func() *plan.TableDef {
		return &plan.TableDef{
			Name: "t1",
			Cols: []*plan.ColDef{
				{
					Name: "a",
					Typ:  plan.Type{Id: int32(types.T_int32)},
					Default: &plan.Default{
						NullAbility: true,
					},
				},
			},
		}
	}

	newClause := func(parts ...*tree.Partition) *tree.AlterPartitionAddPartitionClause {
		return tree.NewAlterPartitionAddPartitionClause(tree.AlterPartitionAddPartition, parts)
	}

	t.Run("parse error on invalid createsql", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "$$$"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
	})

	t.Run("not a create table in createsql", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create view v as select 1"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported ADD PARTITION not in create table")
	})

	t.Run("table without partition option", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create table t1 (a int)"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Partition management on a not partitioned table is not possible")
	})

	t.Run("unsupported method: HASH", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create table t1 (a int) partition by hash(a) partitions 2"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported partition method in ADD PARTITION")
	})

	// RANGE cases (create table has existing one partition p0 < 10)
	rangeCreate := "create table t1 (a int) partition by range (a) (partition p0 values less than (10))"

	t.Run("RANGE: more than one value in values less than", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		v1 := tree.NewNumVal[int64](20, "20", false, tree.P_int64)
		v2 := tree.NewNumVal[int64](30, "30", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesLessThan(tree.Exprs{v1, v2})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RANGE PARTITIONING can only have one parameter")
	})

	t.Run("RANGE: MAXVALUE must be last", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		max := tree.NewMaxValue()
		pMax := &tree.Partition{Name: tree.Identifier("pmax"), Values: tree.NewValuesLessThan(tree.Exprs{max})}
		p2 := &tree.Partition{Name: tree.Identifier("p2"), Values: tree.NewValuesLessThan(tree.Exprs{tree.NewNumVal[int64](20, "20", false, tree.P_int64)})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(pMax, p2))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "MAXVALUE must be the last RANGE partition")
	})

	t.Run("RANGE: values less than must be strictly increasing", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesLessThan(tree.Exprs{tree.NewNumVal[int64](5, "5", false, tree.P_int64)})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "VALUES LESS THAN value must be strictly increasing")
	})

	// LIST cases
	listCreate := "create table t1 (a int) partition by list (a) (partition p0 values in (1))"

	t.Run("LIST: empty values", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LIST PARTITIONING must have at least one value")
	})

	t.Run("LIST: duplicate within same partition", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		v := tree.NewNumVal[int64](2, "2", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{v, v})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate values within the same LIST partition are not allowed")
	})

	t.Run("LIST: duplicate across partitions", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		v := tree.NewNumVal[int64](1, "1", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{v})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LIST PARTITIONING values must be unique across partitions")
	})
}

func TestPartitionCreateSQLIsModeIndependentForAddPartition(t *testing.T) {
	ctx := &sqlModeMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		sqlMode:             "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
	}
	const createSQL = `create table "partition_mode" ("category" varchar(20)) partition by list columns ("category") (partition "select" values in ('A\\B')) cluster by ("category")`
	stmt, err := parsers.ParseOneWithSQLMode(context.Background(), dialect.MYSQL, createSQL, 1, ctx.sqlMode)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	createTablePlan := p.GetDdl().GetCreateTable()
	tableDef := createTablePlan.GetTableDef()
	require.NotNil(t, tableDef)
	for _, def := range tableDef.Defs {
		for _, property := range def.GetProperties().GetProperties() {
			if property.Key == catalog.SystemRelAttr_CreateSQL {
				tableDef.Createsql = property.Value
			}
		}
	}
	require.Contains(t, tableDef.Createsql, "`partition_mode`")
	require.Contains(t, tableDef.Createsql, "`category`")
	require.Contains(t, tableDef.Createsql, "partition `select`")
	require.Contains(t, tableDef.Createsql, "cluster by (`category`)")
	require.Contains(t, tableDef.Createsql, `'A\\\\B'`)
	require.NotContains(t, tableDef.Createsql, `"`)
	require.Equal(t, tableDef.Createsql, createTablePlan.RawSQL)

	newValue := tree.NewNumVal("C\\D", "C\\D", false, tree.P_char)
	clause := tree.NewAlterPartitionAddPartitionClause(
		tree.AlterPartitionAddPartition,
		[]*tree.Partition{{
			Name:   tree.Identifier("p1"),
			Values: tree.NewValuesIn(tree.Exprs{newValue}),
		}},
	)
	defer clause.Free()

	defs, err := constructAddedPartitionDefs(ctx, tableDef, clause)
	require.NoError(t, err)
	require.Len(t, defs, 1)
}

func TestCheckFkColsAreValidRecordsReferencedKey(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.SetContext(context.Background())
	intType := plan.Type{Id: int32(types.T_int32)}
	parent := &TableDef{
		Name: "parent",
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "id", Typ: intType},
			{ColId: 2, Name: "code", Typ: intType},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id", "code"}},
		Indexes: []*plan.IndexDef{
			{IndexName: "uq_parent_id", Unique: true, Parts: []string{"id"}},
			{IndexName: "uq_parent_code", Unique: true, Parts: []string{"code"}},
		},
	}
	newFK := func(columns ...string) *FkData {
		return &FkData{
			ParentTableName: "parent",
			Cols:            &plan.FkColName{Cols: columns},
			ColsReferred:    &plan.FkColName{Cols: columns},
			Def:             &plan.ForeignKeyDef{},
			ColTyps: map[int]*plan.Type{
				0: &intType,
			},
		}
	}

	fk := newFK("id")
	require.NoError(t, checkFkColsAreValid(ctx, fk, parent))
	require.Equal(t, "PRIMARY", fk.Def.ReferencedIndexName)
	require.Equal(t, []uint64{1}, fk.Def.ForeignCols)

	composite := newFK("id", "code")
	composite.ColTyps[1] = &intType
	require.NoError(t, checkFkColsAreValid(ctx, composite, parent))
	require.Equal(t, "PRIMARY", composite.Def.ReferencedIndexName)
	require.Equal(t, []uint64{1, 2}, composite.Def.ForeignCols)

	nonPrefix := newFK("code", "id")
	nonPrefix.ColTyps[1] = &intType
	require.Error(t, checkFkColsAreValid(ctx, nonPrefix, parent), "a non-prefix key must not be accepted")

	unique := newFK("code")
	require.NoError(t, checkFkColsAreValid(ctx, unique, parent))
	require.Equal(t, "uq_parent_code", unique.Def.ReferencedIndexName)
}

func TestCreateExistingTableDoesNotRebuildReverseForeignKeys(t *testing.T) {
	mock := NewMockOptimizer(false)
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

	queriedReverseForeignKeys := false
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			if strings.Contains(sql, "`mo_catalog`.`mo_foreign_keys`") {
				queriedReverseForeignKeys = true
				return executor.Result{}, moerr.NewInternalErrorNoCtx("existing relation reverse FKs must not be rebuilt")
			}
			return executor.Result{}, nil
		}),
	)

	for _, sql := range []string{
		"create table nation (replacement_only int)",
		"create table if not exists nation (replacement_only int)",
	} {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.Empty(t, logicPlan.GetDdl().GetCreateTable().GetFksReferToMe())
	}
	require.False(t, queriedReverseForeignKeys)
}

func TestDropSelectedForeignKeyIndexIsRejected(t *testing.T) {
	for _, referencedIndexName := range []string{"idx1", ""} {
		mode := "persisted name"
		if referencedIndexName == "" {
			mode = "legacy inferred name"
		}
		t.Run(mode, func(t *testing.T) {
			for _, sql := range []string{
				"drop index idx1 on test_idx",
				"alter table test_idx drop index idx1",
			} {
				t.Run(sql, func(t *testing.T) {
					mock := NewMockOptimizer(true)
					parent := mock.ctxt.tables["test_idx"]
					parent.TblId = 100
					parent.Pkey = nil
					parent.RefChildTbls = []uint64{200}
					parent.Indexes = []*plan.IndexDef{
						{IndexName: "idx1", Unique: true, Parts: []string{"n_nationkey"}},
						{IndexName: "idx_alternative", Unique: true, Parts: []string{"n_nationkey"}},
						{IndexName: "idx_unrelated", Unique: true, Parts: []string{"n_name"}},
					}
					child := &TableDef{
						Name:  "fk_child",
						TblId: 200,
						Fkeys: []*plan.ForeignKeyDef{{
							Name:                "fk_child_parent",
							ForeignTbl:          parent.TblId,
							ForeignCols:         []uint64{parent.Cols[0].ColId},
							ReferencedIndexName: referencedIndexName,
						}},
					}
					mock.ctxt.tables[child.Name] = child
					mock.ctxt.objects[child.Name] = &ObjectRef{SchemaName: "tpch", ObjName: child.Name}
					mock.ctxt.id2name[child.TblId] = child.Name

					_, err := runOneStmt(mock, t, sql)
					require.Error(t, err)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDropIndexNeededInForeignKey), err.Error())

					plan, err := runOneStmt(mock, t, "drop index idx_unrelated on test_idx")
					require.NoError(t, err)
					require.Equal(t, "idx_unrelated", plan.GetDdl().GetDropIndex().GetIndexName())

					_, err = runOneStmt(mock, t, "drop index idx_alternative on test_idx")
					if referencedIndexName == "" {
						require.Error(t, err, "legacy metadata must not guess which compatible key was bound")
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrDropIndexNeededInForeignKey), err.Error())
					} else {
						require.NoError(t, err, "a persisted binding makes an alternative key independently droppable")
					}
				})
			}
		})
	}
}

func TestAlterCanDropSelfForeignKeyAndItsSelectedIndexTogether(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["test_idx"]
	tableDef.TblId = 100
	tableDef.Pkey = nil
	tableDef.RefChildTbls = []uint64{0}
	tableDef.Indexes = []*plan.IndexDef{{
		IndexName: "idx1", Unique: true, Parts: []string{"n_nationkey"},
	}}
	tableDef.Fkeys = []*plan.ForeignKeyDef{{
		Name:                "fk_self",
		ForeignTbl:          0,
		ForeignCols:         []uint64{tableDef.Cols[0].ColId},
		ReferencedIndexName: "idx1",
	}}

	logicPlan, err := runOneStmt(mock, t,
		"alter table test_idx drop foreign key fk_self, drop index idx1")
	require.NoError(t, err)
	require.Len(t, logicPlan.GetDdl().GetAlterTable().GetActions(), 2)
}

func TestDropReferencedPrimaryKeyIsRejected(t *testing.T) {
	for _, referencedIndexName := range []string{"PRIMARY", ""} {
		mock := NewMockOptimizer(true)
		parent := mock.ctxt.tables["test_idx"]
		parent.TblId = 100
		parent.RefChildTbls = []uint64{200}
		child := &TableDef{
			Name:  "fk_child_primary",
			TblId: 200,
			Fkeys: []*plan.ForeignKeyDef{{
				Name:                "fk_child_primary",
				ForeignTbl:          parent.TblId,
				ForeignCols:         []uint64{parent.Cols[0].ColId},
				ReferencedIndexName: referencedIndexName,
			}},
		}
		mock.ctxt.tables[child.Name] = child
		mock.ctxt.objects[child.Name] = &ObjectRef{SchemaName: "tpch", ObjName: child.Name}
		mock.ctxt.id2name[child.TblId] = child.Name
		proc := testutil.NewProc(t)
		proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
		mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
		moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
			moruntime.InternalSQLExecutor,
			executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				require.Equal(t,
					"SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = 100", sql)
				result := executor.NewMemResult(
					[]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp(),
				)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(result, 0, []string{"idx1"}))
				require.NoError(t, executor.AppendFixedRows(result, 1, []int8{1}))
				return result.GetResult(), nil
			}),
		)

		_, err := runOneStmt(mock, t, "alter table test_idx drop primary key")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDropIndexNeededInForeignKey), err.Error())
	}
}

func TestCreateForeignKeyUsesLegacyCatalogBeforeTenantUpgrade(t *testing.T) {
	mock := NewMockOptimizer(false)
	legacyColumnNames := []string{
		"constraint_name", "constraint_id", "db_name", "db_id", "table_name", "table_id",
		"column_name", "column_id", "refer_db_name", "refer_db_id", "refer_table_name",
		"refer_table_id", "refer_column_name", "refer_column_id", "on_delete", "on_update",
	}
	legacyCatalog := &TableDef{Name: catalog.MOForeignKeys}
	for _, name := range legacyColumnNames {
		legacyCatalog.Cols = append(legacyCatalog.Cols, &ColDef{Name: name})
	}
	mock.ctxt.tables[catalog.MOForeignKeys] = legacyCatalog

	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	var internalQueries []string
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			internalQueries = append(internalQueries, sql)
			return executor.Result{}, nil
		}),
	)

	logicPlan, err := runOneStmt(mock, t,
		"create table fk_before_upgrade (parent_id int, constraint fk_before_upgrade_parent foreign key (parent_id) references nation(n_nationkey))")
	require.NoError(t, err)
	createTable := logicPlan.GetDdl().GetCreateTable()
	require.Len(t, createTable.UpdateFkSqls, 1)
	require.NotContains(t, createTable.UpdateFkSqls[0], "referenced_index_name")
	require.NotContains(t, createTable.UpdateFkSqls[0], "on_delete_origin")
	require.Len(t, internalQueries, 1)
	require.NotContains(t, internalQueries[0], "referenced_index_name")
	require.NotContains(t, internalQueries[0], "on_delete_origin")
}

func TestForwardForeignKeyCatalogLifecycle(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.SetContext(context.Background())
	ctx.tables[catalog.MOForeignKeys] = &TableDef{
		Name: catalog.MOForeignKeys,
		Cols: []*ColDef{
			{Name: "referenced_index_name"},
			{Name: "on_delete_origin"},
			{Name: "on_update_origin"},
		},
	}
	ctx.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
		if name == "foreign_key_checks" {
			return int64(0), nil
		}
		return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
	}
	intType := plan.Type{Id: int32(types.T_int32)}
	child := &TableDef{
		Name: "child",
		Cols: []*plan.ColDef{{ColId: 1, Name: "parent_id", Typ: intType}},
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table child (parent_id int, constraint fk_child_parent foreign key (parent_id) references parent (id))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	var foreignKey *tree.ForeignKey
	for _, def := range stmt.(*tree.CreateTable).Defs {
		if foreignKey, _ = def.(*tree.ForeignKey); foreignKey != nil {
			break
		}
	}
	require.NotNil(t, foreignKey)

	data, err := getForeignKeyData(ctx, "db", child, foreignKey)
	require.NoError(t, err)
	require.True(t, data.ForwardRefer)
	require.NotEmpty(t, data.UpdateSql, "the child must persist its deferred FK catalog row")
	require.Contains(t, data.UpdateSql, "'fk_child_parent'")
	require.Contains(t, data.UpdateSql, "''", "the parent key is intentionally unresolved at child creation")

	ctx.tables["child"] = child
	parent := &TableDef{
		Name: "parent",
		Cols: []*plan.ColDef{{ColId: 2, Name: "id", Typ: intType}},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
	}
	resolved, err := buildFkDataOfForwardRefer(ctx, "fk_child_parent", []*FkReferDef{{
		Db: "db", Tbl: "child", Col: "parent_id", ReferCol: "id", OnDelete: "NO_ACTION", OnUpdate: "NO_ACTION",
	}}, &plan.CreateTable{Database: "db", TableDef: parent})
	require.NoError(t, err)
	require.Equal(t, "PRIMARY", resolved.Def.ReferencedIndexName)
	require.Equal(t,
		"update `mo_catalog`.`mo_foreign_keys` set referenced_index_name = 'PRIMARY' where db_name = 'db' and table_name = 'child' and constraint_name = 'fk_child_parent'",
		getSqlForUpdateFkReferencedIndex("db", "child", "fk_child_parent", resolved.Def.ReferencedIndexName))
}
