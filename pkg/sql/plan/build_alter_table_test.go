// Copyright 2023 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func newAutoIncrementAlterOptimizer() *MockOptimizer {
	mock := NewMockOptimizer(false)
	mock.ctxt.objects["auto_incr_t"] = &ObjectRef{
		SchemaName: "constraint_test",
		ObjName:    "auto_incr_t",
	}
	mock.ctxt.tables["auto_incr_t"] = &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     24532,
		Name:      "auto_incr_t",
		Cols: []*ColDef{
			{
				ColId:   0,
				Name:    "id",
				Primary: true,
				Typ: plan.Type{
					Id:       int32(types.T_uint64),
					AutoIncr: true,
				},
				Default: &plan.Default{},
			},
			{
				ColId:   1,
				Name:    "v",
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{},
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
		},
	}
	return mock
}

func TestAlterTableAutoIncrementPlan(t *testing.T) {
	for _, tc := range []struct {
		sql        string
		wantOffset uint64
		wantCopy   bool
	}{
		{sql: `ALTER TABLE constraint_test.auto_incr_t AUTO_INCREMENT = 100;`, wantOffset: 99},
		{sql: `ALTER TABLE constraint_test.auto_incr_t AUTO_INCREMENT = 0;`, wantOffset: 0},
		{sql: `ALTER TABLE constraint_test.auto_incr_t AUTO_INCREMENT = 100, ALGORITHM = COPY;`, wantOffset: 99, wantCopy: true},
		{sql: `ALTER TABLE constraint_test.auto_incr_t ADD COLUMN c int, AUTO_INCREMENT = 100;`, wantOffset: 99, wantCopy: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := buildSingleStmt(newAutoIncrementAlterOptimizer(), t, tc.sql)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			if tc.wantCopy {
				require.Equal(t, plan.AlterTable_COPY, alter.AlgorithmType)
				require.Equal(t, tc.wantOffset, alter.CopyTableDef.AutoIncrOffset)
				require.Len(t, alter.Actions, 1)
				require.Equal(t, tc.wantOffset, alter.Actions[0].GetAlterAutoIncrement().NewOffset)
				copied := DeepCopyPlan(p)
				require.Equal(t, tc.wantOffset,
					copied.GetDdl().GetAlterTable().Actions[0].GetAlterAutoIncrement().NewOffset)
				return
			}
			require.Equal(t, plan.AlterTable_INPLACE, alter.AlgorithmType)
			require.Len(t, alter.Actions, 1)
			require.Equal(t, tc.wantOffset, alter.Actions[0].GetAlterAutoIncrement().NewOffset)
			copied := DeepCopyPlan(p)
			require.Equal(t, tc.wantOffset,
				copied.GetDdl().GetAlterTable().Actions[0].GetAlterAutoIncrement().NewOffset)
		})
	}
}

func TestAlterTableAutoIncrementRejectsTableWithoutUserAutoColumn(t *testing.T) {
	_, err := buildSingleStmt(NewMockOptimizer(false), t,
		`ALTER TABLE constraint_test.t1 AUTO_INCREMENT = 100;`)
	require.ErrorContains(t, err, "does not have an AUTO_INCREMENT column")
}

func TestAlterTable1(t *testing.T) {
	//sql := "ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);"
	//sql := "ALTER TABLE t1 ADD d INT NOT NULL PRIMARY KEY;"
	sql := "ALTER TABLE t1 MODIFY b INT;"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestInvisibleColumnClausesAreRejected(t *testing.T) {
	tests := []string{
		`CREATE TABLE visibility_create_invisible (id INT, secret INT INVISIBLE);`,
		`ALTER TABLE t1 ADD COLUMN secret INT INVISIBLE;`,
		`ALTER TABLE t1 MODIFY COLUMN b INT INVISIBLE;`,
		`ALTER TABLE t1 CHANGE COLUMN b b INT INVISIBLE;`,
		`ALTER TABLE t1 ALTER COLUMN b SET INVISIBLE;`,
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := buildSingleStmt(NewMockOptimizer(false), t, sql)
			require.ErrorContains(t, err, "not supported: invisible columns")
		})
	}
}

func TestExplicitVisibleColumnClausesRemainSupported(t *testing.T) {
	for _, sql := range []string{
		`CREATE TABLE visibility_create_visible (id INT VISIBLE);`,
		`ALTER TABLE t1 ALTER COLUMN b SET VISIBLE;`,
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := buildSingleStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)
		})
	}
}

func TestSameNameChangeColumnUsesInplaceAlter(t *testing.T) {
	mock := newMetadataOnlyChangeColumnOptimizer()
	const sql = `ALTER TABLE metadata_only CHANGE v v INT NULL COMMENT 'metadata only';`
	logicPlan, err := buildSingleStmt(
		mock,
		t,
		sql,
	)
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.Equal(t, plan.AlterTable_INPLACE, alter.AlgorithmType)
	require.NotNil(t, alter.CopyTableDef)
	require.Equal(t, "metadata only", FindColumn(alter.CopyTableDef.Cols, "v").Comment)
	require.Empty(t, FindColumn(mock.ctxt.tables["metadata_only"].Cols, "v").Comment)
	require.NotNil(t, alter.Actions[len(alter.Actions)-1].GetAlterReplaceDef())
}

func TestRenameChangeColumnStillUsesCopyAlter(t *testing.T) {
	mock := newMetadataOnlyChangeColumnOptimizer()
	logicPlan, err := buildSingleStmt(
		mock,
		t,
		`ALTER TABLE metadata_only CHANGE v renamed_v INT;`,
	)
	require.NoError(t, err)
	require.Equal(t, plan.AlterTable_COPY, logicPlan.GetDdl().GetAlterTable().AlgorithmType)
}

func TestCaseOnlyChangeColumnUsesCopyAlterAndUpdatesForeignKeyCatalog(t *testing.T) {
	mock := newMetadataOnlyChangeColumnOptimizer()
	logicPlan, err := buildSingleStmt(
		mock,
		t,
		`ALTER TABLE metadata_only CHANGE v V INT;`,
	)
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.Equal(t, plan.AlterTable_COPY, alter.AlgorithmType)
	require.Equal(t, "V", FindColumn(alter.CopyTableDef.Cols, "v").OriginName)
	// These update the child-side column_name and parent-side refer_column_name
	// entries respectively, so either side of an FK relation retains the new
	// spelling after the COPY replacement.
	require.Equal(t, getSqlForRenameColumn("tpch", "metadata_only", "v", "V"), alter.UpdateFkSqls)
}

func newMetadataOnlyChangeColumnOptimizer() *MockOptimizer {
	mock := NewMockOptimizer(false)
	mock.ctxt.objects["metadata_only"] = &ObjectRef{
		SchemaName: "tpch",
		ObjName:    "metadata_only",
	}
	mock.ctxt.tables["metadata_only"] = &TableDef{
		DbName:    "tpch",
		TblId:     987654,
		Name:      "metadata_only",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*ColDef{
			{
				ColId:      1,
				Name:       "id",
				OriginName: "id",
				Primary:    true,
				Typ: plan.Type{
					Id:          int32(types.T_int32),
					NotNullable: true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan.Default{NullAbility: false},
			},
			{
				ColId:      2,
				Name:       "v",
				OriginName: "v",
				Typ:        plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1},
				Default:    &plan.Default{NullAbility: true},
			},
		},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{1},
			Names:       []string{"id"},
		},
		Indexes: []*plan.IndexDef{{
			IndexName:      "idx_v",
			Parts:          []string{"v"},
			IndexTableName: "__mo_index_metadata_only_v",
			TableExist:     true,
		}},
	}
	return mock
}

func TestAlterTableAddColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	// CREATE TABLE t1 (a INTEGER, b CHAR(10));
	sqls := []string{
		`ALTER TABLE t1 ADD d TIMESTAMP;`,
		//`ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY;`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY PRIMARY KEY;`,
		//`ALTER TABLE t2 ADD c INT PRIMARY KEY PRIMARY KEY PRIMARY KEY;`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestAlterTableAddColumnInheritsTableDefaultCharset(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
		want uint32
	}{
		{
			name: "inherits table utf8mb4_bin",
			sql:  "alter table t1 add column d varchar(10)",
			want: uint32(types.CharsetUTF8MB4Bin),
		},
		{
			name: "column collation overrides table",
			sql:  "alter table t1 add column d varchar(10) collate utf8mb4_general_ci",
			want: uint32(types.CharsetUTF8),
		},
		{
			name: "column charset overrides table",
			sql:  "alter table t1 add column d varchar(10) character set utf8mb4",
			want: uint32(types.CharsetUTF8),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["t1"].DefaultCharset = uint32(types.CharsetUTF8MB4Bin)

			logicPlan, err := buildSingleStmt(mock, t, tc.sql)
			assert.NoError(t, err)
			newCol := FindColumn(logicPlan.GetDdl().GetAlterTable().CopyTableDef.Cols, "d")
			if assert.NotNil(t, newCol) {
				assert.Equal(t, tc.want, newCol.Typ.Charset)
			}
		})
	}
}

func TestAlterTableAddColumnOverridesBinaryTableCharsetBeforeTypeConversion(t *testing.T) {
	for _, clause := range []string{
		"character set utf8mb4",
		"collate utf8mb4_general_ci",
	} {
		t.Run(clause, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["t1"].DefaultCharset = uint32(types.CharsetBinary)

			logicPlan, err := buildSingleStmt(mock, t,
				"alter table t1 add column d varchar(10) "+clause)
			if !assert.NoError(t, err) {
				return
			}
			newCol := FindColumn(logicPlan.GetDdl().GetAlterTable().CopyTableDef.Cols, "d")
			if assert.NotNil(t, newCol) {
				assert.Equal(t, int32(types.T_varchar), newCol.Typ.Id)
				assert.Equal(t, uint32(types.CharsetUTF8), newCol.Typ.Charset)
			}
		})
	}
}

func TestAlterTableModifyColumnInheritsTableDefaultCharset(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["t1"].DefaultCharset = uint32(types.CharsetUTF8MB4Bin)

	logicPlan, err := buildSingleStmt(mock, t, "alter table t1 modify column b char(20)")
	assert.NoError(t, err)
	newCol := FindColumn(logicPlan.GetDdl().GetAlterTable().CopyTableDef.Cols, "b")
	if assert.NotNil(t, newCol) {
		assert.Equal(t, uint32(types.CharsetUTF8MB4Bin), newCol.Typ.Charset)
	}
}

func TestAlterTableModifyColumnCharsetOverridesTableDefault(t *testing.T) {
	for _, tc := range []struct {
		name        string
		clause      string
		wantType    int32
		wantCharset uint32
	}{
		{
			name:        "utf8mb4 resets to its default collation",
			clause:      "character set utf8mb4",
			wantType:    int32(types.T_char),
			wantCharset: uint32(types.CharsetUTF8),
		},
		{
			name:        "general ci overrides the binary table charset",
			clause:      "collate utf8mb4_general_ci",
			wantType:    int32(types.T_char),
			wantCharset: uint32(types.CharsetUTF8),
		},
		{
			name:        "binary changes the physical string type",
			clause:      "character set binary",
			wantType:    int32(types.T_binary),
			wantCharset: uint32(types.CharsetBinary),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["t1"].DefaultCharset = uint32(types.CharsetBinary)
			logicPlan, err := buildSingleStmt(mock, t,
				"alter table t1 modify column b char(20) "+tc.clause)
			if !assert.NoError(t, err) {
				return
			}
			newCol := FindColumn(logicPlan.GetDdl().GetAlterTable().CopyTableDef.Cols, "b")
			if assert.NotNil(t, newCol) {
				assert.Equal(t, tc.wantType, newCol.Typ.Id)
				assert.Equal(t, tc.wantCharset, newCol.Typ.Charset)
			}
		})
	}
}

func TestAlterTableAddColumnCharacterSetBinaryChangesType(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t,
		"alter table t1 add column d varchar(10) character set binary")
	if !assert.NoError(t, err) {
		return
	}
	newCol := FindColumn(logicPlan.GetDdl().GetAlterTable().CopyTableDef.Cols, "d")
	if assert.NotNil(t, newCol) {
		assert.Equal(t, int32(types.T_varbinary), newCol.Typ.Id)
		assert.Equal(t, uint32(types.CharsetBinary), newCol.Typ.Charset)
	}
}

func TestAlterTableCopyPreservesFinalColumnReplacementIdentity(t *testing.T) {
	for _, sql := range []string{
		`ALTER TABLE t1 DROP COLUMN b, ADD COLUMN b INT;`,
		`ALTER TABLE t1 RENAME COLUMN b TO tmp, DROP COLUMN tmp, ADD COLUMN b INT;`,
		`ALTER TABLE t1 DROP COLUMN b, ADD COLUMN tmp INT, RENAME COLUMN tmp TO b;`,
	} {
		t.Run(sql, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false), t, sql)
			assert.NoError(t, err)

			alter := logicPlan.GetDdl().GetAlterTable()
			oldCol := FindColumn(alter.TableDef.Cols, "b")
			newCol := FindColumn(alter.CopyTableDef.Cols, "b")
			if assert.NotNil(t, oldCol) && assert.NotNil(t, newCol) {
				assert.NotEqual(t,
					[]uint64{oldCol.ColId, uint64(oldCol.Seqnum)},
					[]uint64{newCol.ColId, uint64(newCol.Seqnum)},
				)
				_, mapped := alter.ChangeTblColIdMap[oldCol.ColId]
				assert.False(t, mapped)
			}
		})
	}
}

func TestAlterTableCopyDoesNotSkipDedupForSameNamePrimaryKeyReplacement(t *testing.T) {
	mock := NewMockOptimizer(false)
	// Match the type metadata produced by ADD COLUMN so the only difference is
	// the source-column identity. A name/type comparison alone must not prove
	// that the replacement key is copied from the old key.
	mock.ctxt.tables["t1"].Cols[0].Typ.NotNullable = false
	mock.ctxt.tables["t1"].Cols[0].Typ.Width = 64
	mock.ctxt.tables["t1"].Cols[0].Typ.Scale = -1
	logicPlan, err := buildSingleStmt(mock, t,
		`ALTER TABLE constraint_test.t1 DROP COLUMN a, ADD COLUMN a BIGINT NOT NULL DEFAULT 0 PRIMARY KEY;`)
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.NotNil(t, alter.Options)
	oldCol := FindColumn(alter.TableDef.Cols, "a")
	newCol := FindColumn(alter.CopyTableDef.Cols, "a")
	require.NotNil(t, oldCol)
	require.NotNil(t, newCol)
	require.Equal(t, oldCol.Typ, newCol.Typ)
	require.NotEqual(t, oldCol.ColId, newCol.ColId)
	_, inherited := alter.ChangeTblColIdMap[oldCol.ColId]
	require.False(t, inherited)
	assert.False(t, alter.Options.SkipPkDedup)
}

func TestAlterTableCopyPreservesExistingColumnIdentity(t *testing.T) {
	for _, tc := range []struct {
		sql              string
		finalName        string
		expectsChangeMap bool
	}{
		{`ALTER TABLE t1 ALGORITHM=COPY, MODIFY COLUMN b VARCHAR(20);`, "b", true},
		{`ALTER TABLE t1 RENAME COLUMN b TO bb;`, "bb", false},
		{`ALTER TABLE t1 ALGORITHM=COPY, RENAME COLUMN b TO bb, MODIFY COLUMN a BIGINT;`, "bb", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false), t, tc.sql)
			assert.NoError(t, err)

			alter := logicPlan.GetDdl().GetAlterTable()
			oldCol := FindColumn(alter.TableDef.Cols, "b")
			newCol := FindColumn(alter.CopyTableDef.Cols, tc.finalName)
			if assert.NotNil(t, oldCol) && assert.NotNil(t, newCol) {
				assert.Equal(t, oldCol.ColId, newCol.ColId)
				assert.Equal(t, oldCol.Seqnum, newCol.Seqnum)
				if tc.expectsChangeMap {
					mapped, ok := alter.ChangeTblColIdMap[oldCol.ColId]
					if assert.True(t, ok, "change map: %#v", alter.ChangeTblColIdMap) {
						assert.Equal(t, tc.finalName, mapped.Name)
					}
				}
			}
		})
	}
}

func TestAlterTableCopyPreservesInvisibleIndex(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := mock.ctxt.tables["t1"]
	tableDef.TblId = 272464
	tableDef.Indexes = []*plan.IndexDef{
		{
			IndexName:      "idx_visible",
			Parts:          []string{"b", catalog.CreateAlias("a")},
			IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
			IndexTableName: "__mo_index_idx_visible",
			TableExist:     true,
			// Legacy default-visible IndexDefs carry the false proto3 zero value.
			Visible: false,
		},
		{
			IndexName:      "idx_invisible",
			Parts:          []string{"b", catalog.CreateAlias("a")},
			IndexAlgo:      catalog.MoIndexDefaultAlgo.ToString(),
			IndexTableName: "__mo_index_idx_invisible",
			TableExist:     true,
			Visible:        false,
		},
	}

	proc := testutil.NewProc(t)
	proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.InternalSQLExecutor,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			require.Equal(t,
				"SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = 272464",
				sql,
			)
			result := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp(),
			)
			result.NewBatchWithRowCount(2)
			require.NoError(t, executor.AppendStringRows(result, 0,
				[]string{"idx_visible", "idx_invisible"}))
			require.NoError(t, executor.AppendFixedRows(result, 1, []int8{1, 0}))
			return result.GetResult(), nil
		}),
	)

	logicPlan, err := buildSingleStmt(mock, t,
		"ALTER TABLE t1 ADD COLUMN added_col INT")
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.Len(t, alter.CopyTableDef.Indexes, 2)
	require.True(t, alter.CopyTableDef.Indexes[0].Visible)
	require.False(t, alter.CopyTableDef.Indexes[1].Visible)
	require.NotContains(t, alter.CreateTmpTableSql, "KEY `idx_visible` (`b`) INVISIBLE")
	require.Contains(t, alter.CreateTmpTableSql, "KEY `idx_invisible` (`b`) INVISIBLE")
}

func TestAlterTableCopyDropsEveryAdjacentIndexForDroppedColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.tables["t1"].Indexes = []*plan.IndexDef{
		{
			IndexName:  "uk_b",
			Parts:      []string{"b"},
			Unique:     true,
			TableExist: true,
		},
		{
			IndexName:  "idx_b",
			Parts:      []string{"b", catalog.CreateAlias("a")},
			IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
			TableExist: true,
		},
	}

	logicPlan, err := buildSingleStmt(mock, t, "ALTER TABLE t1 DROP COLUMN b")
	require.NoError(t, err)

	alter := logicPlan.GetDdl().GetAlterTable()
	require.Empty(t, alter.CopyTableDef.Indexes)
	require.NotContains(t, alter.CreateTmpTableSql, "KEY `uk_b`")
	require.NotContains(t, alter.CreateTmpTableSql, "KEY `idx_b`")
}

func TestAlterTableRejectsNonGeometrySRIDAttribute(t *testing.T) {
	mock := NewMockOptimizer(false)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "add non-geometry srid column",
			sql:  "alter table t1 add column d int srid 4326;",
		},
		{
			name: "modify non-geometry srid column",
			sql:  "alter table t1 modify column b int srid 4326;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildSingleStmt(mock, t, tt.sql)
			assert.ErrorContains(t, err, "SRID is only supported for GEOMETRY columns")
		})
	}
}

func TestBuildNotNullColumnValGeometry(t *testing.T) {
	tests := []struct {
		name string
		typ  plan.Type
		want string
	}{
		{
			name: "point",
			typ:  *geometryPlanType(types.T_geometry, "POINT", 0, false),
			want: "st_geomfromtext('POINT EMPTY')",
		},
		{
			name: "point with srid",
			typ:  *geometryPlanType(types.T_geometry, "POINT", 4326, true),
			want: "st_geomfromtext('POINT EMPTY', 4326)",
		},
		{
			name: "generic geometry",
			typ:  *geometryPlanType(types.T_geometry, "GEOMETRY", 0, false),
			want: "st_geomfromtext('GEOMETRYCOLLECTION EMPTY')",
		},
		{
			name: "multipolygon with srid",
			typ:  *geometryPlanType(types.T_geometry, "MULTIPOLYGON", 0, true),
			want: "st_geomfromtext('MULTIPOLYGON EMPTY', 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &ColDef{Name: "g", Typ: tt.typ}
			assert.Equal(t, tt.want, buildNotNullColumnVal(col))
		})
	}
}

func Test_checkChangeTypeCompatible(t *testing.T) {
	type args struct {
		ctx    context.Context
		origin *plan.Type
		to     *plan.Type
	}

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "binary to json still rejected for ddl",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_binary)},
				to:     &plan.Type{Id: int32(types.T_json)},
			},
			wantErr: assert.Error,
		},
		{
			name: "varchar to json remains allowed for ddl",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_varchar)},
				to:     &plan.Type{Id: int32(types.T_json)},
			},
			wantErr: assert.NoError,
		},
		{
			name: "int to json rejected for ddl despite expression cast support",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_int32)},
				to:     &plan.Type{Id: int32(types.T_json)},
			},
			wantErr: assert.Error,
		},
		{
			name: "test3",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_enum)},
				to:     &plan.Type{Id: int32(types.T_varchar)},
			},
			wantErr: assert.NoError,
		},
		{
			name: "test4",
			args: args{
				ctx:    context.Background(),
				origin: &plan.Type{Id: int32(types.T_varchar)},
				to:     &plan.Type{Id: int32(types.T_enum)},
			},
			wantErr: assert.NoError,
		},
		{
			name: "geometry subtype mismatch",
			args: args{
				ctx:    context.Background(),
				origin: geometryPlanType(types.T_geometry, "POINT", 0, false),
				to:     geometryPlanType(types.T_geometry, "LINESTRING", 0, false),
			},
			wantErr: assert.Error,
		},
		{
			name: "geometry generic target accepts subtype",
			args: args{
				ctx:    context.Background(),
				origin: geometryPlanType(types.T_geometry, "POINT", 0, false),
				to:     geometryPlanType(types.T_geometry, "GEOMETRY", 0, false),
			},
			wantErr: assert.NoError,
		},
		{
			name: "geometry identical subtype",
			args: args{
				ctx:    context.Background(),
				origin: geometryPlanType(types.T_geometry, "POINT", 0, false),
				to:     geometryPlanType(types.T_geometry, "POINT", 0, false),
			},
			wantErr: assert.NoError,
		},
		{
			name: "geometry srid mismatch",
			args: args{
				ctx:    context.Background(),
				origin: geometryPlanType(types.T_geometry, "POINT", 4326, true),
				to:     geometryPlanType(types.T_geometry, "POINT", 0, true),
			},
			wantErr: assert.Error,
		},
		{
			name: "geometry identical srid",
			args: args{
				ctx:    context.Background(),
				origin: geometryPlanType(types.T_geometry, "POINT", 4326, true),
				to:     geometryPlanType(types.T_geometry, "POINT", 4326, true),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, checkChangeTypeCompatible(tt.args.ctx, tt.args.origin, tt.args.to), fmt.Sprintf("checkChangeTypeCompatible(%v, %v, %v)", tt.args.ctx, tt.args.origin, tt.args.to))
		})
	}
}

func buildSingleStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	plan, err := BuildPlan(context, statements[0], false)
	if plan != nil {
		testDeepCopy(plan)
	}
	return plan, err
}

// TestAlterTableVarcharLengthBumped tests the isVarcharLengthBumped function.
func TestAlterTableVarcharLengthBumped(t *testing.T) {
	tests := []struct {
		name     string
		clause   *tree.AlterTableModifyColumnClause
		tableDef *TableDef
		wantOk   bool
		wantErr  bool
	}{
		{
			name: "varchar length increased",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},
		{
			name: "varchar length decreased",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  50,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "column not found",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col_not_exist"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: true,
		},
		{
			name: "different type",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "char",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "position changed",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
				},
				Position: &tree.ColumnPosition{
					Typ:            tree.ColumnPositionAfter,
					RelativeColumn: tree.NewUnresolvedColName("col2"),
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
					{
						Name:  "col2",
						ColId: 2,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
		{
			name: "with null attribute matching",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: false},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: false,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},
		{
			name: "with null attribute dropped",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: true},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: false,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},

		{
			name: "with null attribute add not null",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: false},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: true,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},

		{
			name: "with comment or default attributes",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeComment{
							CMT: tree.NewStrVal("default value: test"),
						},
						&tree.AttributeDefault{
							Expr: tree.NewStrVal("test"),
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: true,
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},

		{
			name: "with on update attribute",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeOnUpdate{
							Expr: tree.NewStrVal("CURRENT_TIMESTAMP"),
						},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: true,
						},
						OnUpdate: &plan.OnUpdate{
							OriginString: "CURRENT_TIMESTAMP",
						},
					},
				},
			},
			wantOk:  true,
			wantErr: false,
		},

		{
			name: "with key attribute",
			clause: &tree.AlterTableModifyColumnClause{
				NewColumn: &tree.ColumnTableDef{
					Name: tree.NewUnresolvedColName("col1"),
					Type: &tree.T{
						InternalType: tree.InternalType{
							FamilyString: "varchar",
							Oid:          uint32(defines.MYSQL_TYPE_VARCHAR),
							DisplayWith:  200,
						},
					},
					Attributes: []tree.ColumnAttribute{
						&tree.AttributeNull{Is: true},
						&tree.AttributeKey{},
					},
				},
			},
			tableDef: &TableDef{
				Cols: []*ColDef{
					{
						Name:  "col1",
						ColId: 1,
						Typ: plan.Type{
							Id:    int32(types.T_varchar),
							Width: 100,
						},
						Default: &plan.Default{
							NullAbility: true,
						},
					},
				},
			},
			wantOk:  false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ok, err := isInplaceModifyColumn(ctx, tt.clause, tt.tableDef)
			if (err != nil) != tt.wantErr {
				t.Errorf("isVarcharLengthBumped() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if ok != tt.wantOk {
				t.Errorf("isVarcharLengthBumped() = %v, want %v", ok, tt.wantOk)
			}
		})
	}
}

func TestAlterTableAlgorithmValidation(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("COPY with matching ALGORITHM", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=COPY, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=DEFAULT, ADD COLUMN x INT;`,
		}
		runTestShouldPass(mock, t, sqls, false, false)
	})

	t.Run("COPY with conflicting ALGORITHM", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=INPLACE, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=INSTANT, DROP COLUMN b;`,
		}
		runTestShouldError(mock, t, sqls)
	})

	t.Run("INPLACE-eligible operations accept ALGORITHM=INPLACE and ALGORITHM=INSTANT", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=INPLACE, ADD INDEX idx_a(a);`,
			`ALTER TABLE t1 ALGORITHM=INSTANT, ADD INDEX idx_a(a);`,
			`ALTER TABLE t1 ALGORITHM=DEFAULT, ADD INDEX idx_a(a);`,
		}
		runTestShouldPass(mock, t, sqls, false, false)
	})

	t.Run("INPLACE-eligible operations reject ALGORITHM=COPY", func(t *testing.T) {
		_, err := buildSingleStmt(mock, t,
			`ALTER TABLE t1 ALGORITHM=COPY, ADD INDEX idx_a(a);`)
		assert.ErrorContains(t, err, "unsupported alter option in copy mode")
	})

	t.Run("COPY with LOCK=NONE", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 LOCK=NONE, ADD COLUMN x INT;`,
		}
		runTestShouldError(mock, t, sqls)
	})

	t.Run("COPY with LOCK=SHARED/EXCLUSIVE", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 LOCK=SHARED, ADD COLUMN x INT;`,
			`ALTER TABLE t1 LOCK=EXCLUSIVE, ADD COLUMN x INT;`,
		}
		runTestShouldPass(mock, t, sqls, false, false)
	})

	t.Run("ALGORITHM conflict takes priority over LOCK", func(t *testing.T) {
		_, err := buildSingleStmt(mock, t,
			`ALTER TABLE t1 ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN x INT;`)
		assert.ErrorContains(t, err, "ALGORITHM")
	})

	t.Run("repeated ALGORITHM hints on INPLACE operation, last hint wins", func(t *testing.T) {
		// Multiple ALGORITHM clauses on an INPLACE-capable ADD INDEX:
		// the last hint determines the final algorithm.
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=COPY, ALGORITHM=INPLACE, ADD INDEX idx_a(a);`,
			`ALTER TABLE t1 ALGORITHM=INSTANT, ALGORITHM=INPLACE, ADD INDEX idx_a(a);`,
			`ALTER TABLE t1 ALGORITHM=INPLACE, ALGORITHM=DEFAULT, ADD INDEX idx_a(a);`,
		}
		runTestShouldPass(mock, t, sqls, false, false)
	})

	t.Run("repeated ALGORITHM hints on INPLACE operation, last hint COPY rejected", func(t *testing.T) {
		// ALGORITHM=INPLACE then ALGORITHM=COPY on ADD INDEX: last hint (COPY)
		// routes through buildAlterTableCopy which does not support ADD INDEX.
		_, err := buildSingleStmt(mock, t,
			`ALTER TABLE t1 ALGORITHM=INPLACE, ALGORITHM=COPY, ADD INDEX idx_a(a);`)
		assert.ErrorContains(t, err, "unsupported alter option in copy mode")
	})

	t.Run("repeated ALGORITHM hints on COPY-required operation, non-COPY rejected", func(t *testing.T) {
		// ADD COLUMN requires COPY. All non-COPY hints are rejected against
		// the stable requiredAlgorithm, regardless of what prior hints set.
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=INSTANT, ALGORITHM=INPLACE, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=INPLACE, ALGORITHM=COPY, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=COPY, ALGORITHM=INSTANT, ADD COLUMN x INT;`,
		}
		runTestShouldError(mock, t, sqls)
	})

	t.Run("repeated ALGORITHM hints on COPY-required operation, all COPY passes", func(t *testing.T) {
		sqls := []string{
			`ALTER TABLE t1 ALGORITHM=COPY, ALGORITHM=COPY, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=DEFAULT, ALGORITHM=COPY, ADD COLUMN x INT;`,
			`ALTER TABLE t1 ALGORITHM=COPY, ALGORITHM=DEFAULT, ADD COLUMN x INT;`,
		}
		runTestShouldPass(mock, t, sqls, false, false)
	})
}
