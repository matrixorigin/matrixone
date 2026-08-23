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
