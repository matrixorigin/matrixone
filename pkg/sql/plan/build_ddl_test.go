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
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type rootSQLCompilerContext struct {
	*MockCompilerContext
	rootSQL string
	calls   int
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

func (c *rootSQLCompilerContext) GetRootSql() string {
	c.calls++
	return c.rootSQL
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
	require.Equal(t, "renamed_flags", cols[1].GetName())
	require.Equal(t, int32(types.T_uint64), flagsType.GetId())
	require.Equal(t, "red,green,blue", flagsType.GetEnumvalues())
	require.False(t, flagsType.GetNotNullable())
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
	require.True(t, isSetPlanType(&cols[1].Typ))
	require.Equal(t, "red,green,blue", cols[1].Typ.GetEnumvalues())
	require.Equal(t, int32(types.T_varchar), cols[2].Typ.GetId())
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
	ctx.tables["nation"].Cols[0].Default = &plan.Default{OriginString: "'ALGERIA'"}

	tests := []struct {
		name              string
		sql               string
		wantState         ProvenanceState
		wantDefault       string
		canInheritDefault bool
	}{
		{name: "direct", sql: "select n_nationkey from nation", wantState: ProvenanceSingleSource, wantDefault: "'ALGERIA'", canInheritDefault: true},
		{name: "alias derived", sql: "select k from (select n_nationkey as k from nation) d", wantState: ProvenanceSingleSource, wantDefault: "'ALGERIA'"},
		{name: "non recursive cte", sql: "with d as (select n_nationkey as k from nation) select k from d", wantState: ProvenanceSingleSource, wantDefault: "'ALGERIA'"},
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
				require.Equal(t, test.wantDefault, provenance.Source.Metadata.DefaultOriginString)
				require.Equal(t, test.canInheritDefault, provenance.CanInheritSourceDefault)
				require.NotZero(t, provenance.Source.RelPos)
			} else {
				require.Nil(t, provenance.Source)
			}
		})
	}
}

func TestBuildCTASConsumesOutputColumnProvenance(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.tables["nation"].Cols[0].Default = &plan.Default{OriginString: "'ALGERIA'"}

	tests := []struct {
		name        string
		selectSQL   string
		wantDefault string
	}{
		{name: "direct alias", selectSQL: "select n_nationkey as k from nation", wantDefault: "'ALGERIA'"},
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
		})
	}
}

func TestOutputColumnProvenanceSnapshotsCatalogMetadataOnce(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	addMySQLSpecialTypeColumns(ctx)
	priorityCol := ctx.tables["nation"].Cols[len(ctx.tables["nation"].Cols)-2]
	priorityCol.Default = &plan.Default{OriginString: "'low'"}

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
	require.Equal(t, "low,medium,high", provenance.Source.Metadata.Typ.Enumvalues)
	require.True(t, provenance.Source.Metadata.HasDefault)
	require.Equal(t, "'low'", provenance.Source.Metadata.DefaultOriginString)
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
	_, err = buildAlterView(stmt1.(*tree.AlterView), ctx)
	assert.NoError(t, err)
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
