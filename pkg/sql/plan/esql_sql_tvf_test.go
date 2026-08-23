// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
	"github.com/stretchr/testify/require"
)

func TestBuildForeignTVF(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		// schema mode, long format
		`select * from sql_tvf('select 1', '{"cols":[{"name":"a","type":"int64"},{"name":"b","type":"string"}]}') x`,
		`select * from esql_tvf('FROM idx | LIMIT 5', '{"cols":[{"name":"a","type":"int64"}]}') x`,
		// schema mode, short format
		`select * from sql_tvf('select 1', 'Is') x`,
		// no schema / NULL schema -> single json column
		`select * from sql_tvf('select 1') x`,
		`select * from esql_tvf('FROM idx', NULL) x`,
		// with a conn argument (session var is a runtime expr)
		`select * from sql_tvf('select 1', 'I', @h) x`,
		`select * from esql_tvf('FROM idx', NULL, @h) x`,
		// projection over declared schema columns
		`select a from sql_tvf('select 1', '{"cols":[{"name":"a","type":"int64"},{"name":"b","type":"string"}]}') x`,
	}
	runTestShouldPass(mock, t, sqls, false, false)

	errSqls := []string{
		// no arguments / too many arguments
		`select * from sql_tvf() x`,
		`select * from sql_tvf('q', 'I', @h, 'extra') x`,
		// correlated runtime input (comma-join and CROSS APPLY spellings): the
		// scan must stay on the session CN, so column-referencing arguments
		// are rejected at bind time — including nested inside functions.
		`select f.col0 from nation n, sql_tvf(n.n_name, 'I', @h) f`,
		`select f.col0 from nation n cross apply sql_tvf(n.n_name, 'I', @h) f`,
		`select f.col0 from nation n cross apply sql_tvf(concat(n.n_name, ''), 'I', @h) f`,
		`select f.col0 from nation n cross apply esql_tvf(n.n_name, 'I', @h) f`,
		// non-string runtime arguments
		`select * from sql_tvf(1, 'I') x`,
		`select * from sql_tvf('q', 'I', 42) x`,
		// schema must be a constant literal
		`select * from sql_tvf('q', @schema_var) x`,
		// malformed schema
		`select * from sql_tvf('q', 'Z') x`,
		`select * from sql_tvf('q', '{"cols":[{"name":"a","type":"nosuch"}]}') x`,
	}
	runTestShouldError(mock, t, errSqls)
}

func TestParseTVFColumnSchema(t *testing.T) {
	ctx := context.Background()

	// short format maps every type character.
	opts, err := parseTVFColumnSchema(ctx, "biIfFst")
	require.NoError(t, err)
	require.Len(t, opts.Cols, 7)
	require.Equal(t, ParseJsonlFormatArray, opts.Format)
	wantTypes := []string{
		ParseJsonlTypeBool, ParseJsonlTypeInt32, ParseJsonlTypeInt64,
		ParseJsonlTypeFloat32, ParseJsonlTypeFloat64, ParseJsonlTypeString,
		ParseJsonlTypeTimestamp,
	}
	for i, w := range wantTypes {
		require.Equal(t, w, opts.Cols[i].Type)
	}

	// long format keeps names.
	opts, err = parseTVFColumnSchema(ctx, `{"format":"array","cols":[{"name":"x","type":"int64"}]}`)
	require.NoError(t, err)
	require.Len(t, opts.Cols, 1)
	require.Equal(t, "x", opts.Cols[0].Name)

	// invalid short character errors.
	_, err = parseTVFColumnSchema(ctx, "bZ")
	require.Error(t, err)
	// invalid JSON errors.
	_, err = parseTVFColumnSchema(ctx, `{"cols": nope}`)
	require.Error(t, err)
}

func TestBuildTVFColDefs(t *testing.T) {
	ctx := context.Background()
	opts := ParseJsonlOptions{Cols: []ParseJsonlOptionsCol{
		{Name: "b", Type: ParseJsonlTypeBool},
		{Name: "s", Type: ParseJsonlTypeString},
	}}
	cols, err := buildTVFColDefs(ctx, opts)
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Equal(t, int32(types.T_bool), cols[0].Typ.Id)
	require.Equal(t, int32(types.T_varchar), cols[1].Typ.Id)

	// unknown type name is rejected, not silently untyped.
	_, err = buildTVFColDefs(ctx, ParseJsonlOptions{Cols: []ParseJsonlOptionsCol{{Name: "x", Type: "nosuch"}}})
	require.Error(t, err)

	// duplicate column names would silently alias two outputs to one source
	// field position in the foreign TVF mapping; rejected instead.
	_, err = buildTVFColDefs(ctx, ParseJsonlOptions{Cols: []ParseJsonlOptionsCol{
		{Name: "a", Type: ParseJsonlTypeInt64}, {Name: "a", Type: ParseJsonlTypeString}}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate column name")
}

func TestForeignTVFParamRoundTrip(t *testing.T) {
	p := ForeignTVFParam{Kind: ForeignTVFKindSQL, NoSchema: false,
		Cols: []ParseJsonlOptionsCol{{Name: "a", Type: "int64"}}}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	var q ForeignTVFParam
	require.NoError(t, json.Unmarshal(data, &q))
	require.Equal(t, p, q)
}

// TestFormatForeignTableOptionsForShowCreate pins SHOW CREATE emission:
// inline config redacted, env: verbatim, query emitted, optionless minimal.
func TestFormatForeignTableOptionsForShowCreate(t *testing.T) {
	got := formatForeignTableOptionsForShowCreate(foreignext.Config{
		Kind: "sql", ConfigJSON: `{"driver":"mysql","dsn":"u:pw@h/db"}`, DefaultQuery: "select 1",
	}, "")
	require.Equal(t, ` ENGINE = SQL WITH ("config" = '<redacted>', "query" = 'select 1')`, got)
	require.NotContains(t, got, "pw@h")

	got = formatForeignTableOptionsForShowCreate(foreignext.Config{
		Kind: "esql", ConfigJSON: "env:ES_CFG",
	}, "")
	require.Equal(t, ` ENGINE = ESQL WITH ("config" = 'env:ES_CFG')`, got)

	got = formatForeignTableOptionsForShowCreate(foreignext.Config{Kind: "esql"}, "")
	require.Equal(t, ` ENGINE = ESQL`, got)
}

// TestIsForeignTableDef covers the envelope/feature-bit cross-check.
func TestIsForeignTableDef(t *testing.T) {
	ctx := context.Background()
	env := foreignext.BuildCreateSQLEnvelope(foreignext.Config{Kind: "sql", ConfigJSON: "env:X"})

	// nil / non-external table
	_, ok, err := IsForeignTableDef(ctx, nil)
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = IsForeignTableDef(ctx, &TableDef{TableType: "r"})
	require.NoError(t, err)
	require.False(t, ok)

	// envelope + flag agree
	def := &TableDef{TableType: catalog.SystemExternalRel, Createsql: env,
		FeatureFlag: features.ForeignExternal}
	cfg, ok, err := IsForeignTableDef(ctx, def)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "sql", cfg.Kind)

	// envelope without the feature flag: forgery -> error
	def = &TableDef{TableType: catalog.SystemExternalRel, Createsql: env}
	_, _, err = IsForeignTableDef(ctx, def)
	require.ErrorContains(t, err, "without the foreign feature flag")

	// flag without the envelope: corrupted -> error
	def = &TableDef{TableType: catalog.SystemExternalRel, Createsql: "{}",
		FeatureFlag: features.ForeignExternal}
	_, _, err = IsForeignTableDef(ctx, def)
	require.ErrorContains(t, err, "missing its catalog envelope")

	// neither: an ordinary external table
	def = &TableDef{TableType: catalog.SystemExternalRel, Createsql: "{}"}
	_, ok, err = IsForeignTableDef(ctx, def)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestBuildParseJsonlSpecs covers the refactored parse_jsonl schema dispatch
// (shared helper + jsonl-only format validation) through the binder.
func TestBuildParseJsonlSpecs(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		`select * from parse_jsonl_data('[1]', 'I') x`,
		`select * from parse_jsonl_data('{"a":1}', '{"format":"object","cols":[{"name":"a","type":"int64"}]}') x`,
		`select * from parse_jsonl_data('[1]') x`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
	errSqls := []string{
		`select * from parse_jsonl_data('[1]', 'Z') x`,
		`select * from parse_jsonl_data('[1]', '{"format":"nope","cols":[{"name":"a","type":"int64"}]}') x`,
		`select * from parse_jsonl_data('[1]', '{"cols":[{"name":"a","type":"nosuch"}]}') x`,
	}
	runTestShouldError(mock, t, errSqls)
}

// TestBuildCreateForeignTable drives the CREATE EXTERNAL TABLE ... ENGINE =
// ESQL|SQL DDL branch: option validation, inline-config JSON validation, the
// envelope + feature-flag stamping, and the error cases.
func TestBuildCreateForeignTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		`create external table t1 (a int, b varchar(10)) engine = sql with ('config'='{"driver":"mysql","dsn":"u@h/db"}', 'query'='select 1')`,
		`create external table t2 (a int) engine = esql with ('config'='env:ES_CFG')`,
		`create external table t3 (a int) engine = esql`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
	errSqls := []string{
		// unknown option
		`create external table t4 (a int) engine = sql with ('recheck'='true')`,
		// bad inline config JSON shape
		`create external table t5 (a int) engine = sql with ('config'='{"driver":"nope","dsn":"x"}')`,
		`create external table t6 (a int) engine = esql with ('config'='{}')`,
		// SHOW CREATE replay of a redacted config
		`create external table t7 (a int) engine = sql with ('config'='<redacted>')`,
	}
	runTestShouldError(mock, t, errSqls)
}

// TestSelectAndAlterForeignTable injects a foreign external table into the
// mock catalog and drives the SELECT-side recognition (FOREIGN_TB dispatch +
// hidden __mo_query column) and the ALTER guard.
func TestSelectAndAlterForeignTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	mcc := mock.CurrentContext().(*MockCompilerContext)
	env := foreignext.BuildCreateSQLEnvelope(foreignext.Config{Kind: "sql", ConfigJSON: "env:X"})
	mcc.tables["foreign_t"] = &TableDef{
		TableType:   catalog.SystemExternalRel,
		TblId:       990001,
		Name:        "foreign_t",
		Createsql:   env,
		FeatureFlag: features.ForeignExternal,
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", ColId: 2, Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
		},
	}
	mcc.objects["foreign_t"] = &ObjectRef{SchemaName: "tpch", ObjName: "foreign_t", Obj: 990001}

	// SELECT recognition: FOREIGN_TB extern scan with the hidden column usable
	// in predicates and projection.
	sqls := []string{
		`select a, b from foreign_t where __mo_query = 'select 1'`,
		`select a, __mo_query from foreign_t where __mo_query in ('q1', 'q2')`,
		`select * from foreign_t where __mo_query = 'q'`, // * hides __mo_query
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// the built plan really is a FOREIGN_TB extern scan
	p, err := runOneStmt(mock, t, `select a from foreign_t where __mo_query = 'q'`)
	require.NoError(t, err)
	var found bool
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == plan.Node_EXTERNAL_SCAN {
			require.Equal(t, int32(plan.ExternType_FOREIGN_TB), node.ExternScan.Type)
			require.Equal(t, "sql", node.ExternScan.ForeignScan.Kind)
			last := node.TableDef.Cols[len(node.TableDef.Cols)-1]
			require.Equal(t, catalog.ExternalQuery, last.Name)
			require.Equal(t, catalog.ExternalQueryColId, last.ColId)
			found = true
		}
	}
	require.True(t, found)

	// ALTER is cleanly rejected.
	runTestShouldError(mock, t, []string{`alter table foreign_t add column c int`})

	// reserved hidden-column names are rejected on CREATE and ALTER ADD.
	runTestShouldError(mock, t, []string{
		`create table t_res (a int, __mo_query varchar(10))`,
		`create table t_res2 (a int, __mo_filepath varchar(10))`,
		`alter table nation add column __mo_query varchar(10)`,
	})
}
