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

// Unit coverage for the fulltext2 compile hooks and their param resolvers. The stub
// CompileContext records the SQL each hook emits instead of running it.
package compile

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// stubCtx records every SQL the hooks emit and every CDC/idxcron registration.
type stubCtx struct {
	isFrontend   bool
	experimental bool
	expErr       error
	runErr       error

	sqls        []string
	dropped     int
	created     int
	registered  int
	origTable   *plan.TableDef
	qryDatabase string
}

func newStubCtx() *stubCtx {
	return &stubCtx{experimental: true, qryDatabase: "db", origTable: srcTableDef()}
}

func (s *stubCtx) Ctx() compileplugin.Context       { return nil }
func (s *stubCtx) Database() engine.Database        { return nil }
func (s *stubCtx) QryDatabase() string              { return s.qryDatabase }
func (s *stubCtx) OriginalTableDef() *plan.TableDef { return s.origTable }
func (s *stubCtx) IndexInfo() *plan.CreateTable     { return nil }
func (s *stubCtx) MainTableID() uint64              { return 0 }
func (s *stubCtx) MainExtra() *api.SchemaExtra      { return nil }
func (s *stubCtx) RunSql(sql string) error {
	s.sqls = append(s.sqls, sql)
	return s.runErr
}
func (s *stubCtx) BuildIndexTable(_ *plan.TableDef) error { return nil }
func (s *stubCtx) ResolveVariable(_ string, _, _ bool) (any, error) {
	return int64(0), nil
}
func (s *stubCtx) IsFrontend() bool   { return s.isFrontend }
func (s *stubCtx) IsTableClone() bool { return false }
func (s *stubCtx) IsExperimentalEnabled(_ string) (bool, error) {
	return s.experimental, s.expErr
}
func (s *stubCtx) IsCCPRTaskTransaction() bool                  { return false }
func (s *stubCtx) IsTableFromPublication(_ *plan.TableDef) bool { return false }
func (s *stubCtx) SinkerTypeFromAlgo(_ string) int8             { return 0 }
func (s *stubCtx) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	s.created++
	return nil
}
func (s *stubCtx) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	s.dropped++
	return nil
}
func (s *stubCtx) RunSqlWithResult(_ string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (s *stubCtx) RegisterIdxcronUpdate(_ uint64, _, _, _, _ string, _ []byte) error {
	s.registered++
	return nil
}

var _ compileplugin.CompileContext = (*stubCtx)(nil)

// srcTableDef is a source table with a pk, a text column and a payload column.
func srcTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name:  "src",
		TblId: 42,
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "body"},
			{Name: "payload"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "body": 1, "payload": 2},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
}

func indexDefs(algoParams string) map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		catalog.FullText2Index_TblType_Storage: {
			IndexName:       "idx",
			IndexTableName:  "__mo_store",
			IndexAlgoParams: algoParams,
			Parts:           []string{"body"},
		},
		catalog.FullText2Index_TblType_Metadata: {
			IndexName:      "idx",
			IndexTableName: "__mo_meta",
		},
	}
}

// --- param readers ---------------------------------------------------------

func TestParserFromParams(t *testing.T) {
	require.Equal(t, "", parserFromParams(""))
	require.Equal(t, "", parserFromParams("not json"), "malformed params fall back to the default parser")
	require.Equal(t, "", parserFromParams(`{}`))
	require.Equal(t, "ngram", parserFromParams(`{"parser":"ngram"}`))
}

// include_keys is the json word breaker's persisted shape; absence means keys on.
func TestJSONTermShapeFromParams(t *testing.T) {
	require.False(t, jsonTermShapeFromParams(""))
	require.False(t, jsonTermShapeFromParams("not json"))
	require.False(t, jsonTermShapeFromParams(`{}`))
	require.False(t, jsonTermShapeFromParams(`{"include_keys":"true"}`))
	require.True(t, jsonTermShapeFromParams(`{"include_keys":"false"}`))
}

func TestResolveFulltext2Capacity(t *testing.T) {
	for _, c := range []struct {
		name   string
		params string
		want   int64
	}{
		{"absent", "", DefaultMaxIndexCapacity},
		{"omitted option", `{"parser":"ngram"}`, DefaultMaxIndexCapacity},
		{"empty value", `{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":""}`, DefaultMaxIndexCapacity},
		{"zero falls back", `{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":"0"}`, DefaultMaxIndexCapacity},
		{"set", `{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":"5000"}`, 5000},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveFulltext2Capacity(c.params)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	_, err := resolveFulltext2Capacity(`{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":"abc"}`)
	require.Error(t, err, "a non-numeric cap is an error, not a silent default")
}

func TestResolveFulltext2PostingCapacity(t *testing.T) {
	for _, c := range []struct {
		name   string
		params string
		want   int64
	}{
		{"absent", "", DefaultMaxPostingsCapacity},
		{"omitted option", `{"parser":"ngram"}`, DefaultMaxPostingsCapacity},
		{"empty value", `{"` + catalog.IndexAlgoParamMaxPostingsCapacity + `":""}`, DefaultMaxPostingsCapacity},
		{"zero falls back", `{"` + catalog.IndexAlgoParamMaxPostingsCapacity + `":"0"}`, DefaultMaxPostingsCapacity},
		{"set", `{"` + catalog.IndexAlgoParamMaxPostingsCapacity + `":"77"}`, 77},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveFulltext2PostingCapacity(c.params)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	_, err := resolveFulltext2PostingCapacity(`{"` + catalog.IndexAlgoParamMaxPostingsCapacity + `":"abc"}`)
	require.Error(t, err)
}

func TestResolveFulltext2PositionFree(t *testing.T) {
	for _, c := range []struct {
		params string
		want   bool
	}{
		{"", false},
		{`{}`, false},
		{`{"` + catalog.IndexAlgoParamPositionFree + `":"false"}`, false},
		{`{"` + catalog.IndexAlgoParamPositionFree + `":"true"}`, true},
	} {
		got, err := resolveFulltext2PositionFree(c.params)
		require.NoError(t, err)
		require.Equal(t, c.want, got)
	}
}

// --- INCLUDE column resolution ---------------------------------------------

// The first-class field wins; algo_params is the reload-safe fallback.
func TestFulltext2IncludeColumns(t *testing.T) {
	require.Nil(t, fulltext2IncludeColumns(&plan.IndexDef{}))
	require.Equal(t, []string{"a"},
		fulltext2IncludeColumns(&plan.IndexDef{IncludedColumns: []string{"a"}}))

	raw, err := catalog.IndexParamsMapToJsonString(map[string]string{catalog.IncludedColumns: "payload"})
	require.NoError(t, err)
	require.Equal(t, []string{"payload"},
		fulltext2IncludeColumns(&plan.IndexDef{IndexAlgoParams: raw}))

	require.Nil(t, fulltext2IncludeColumns(&plan.IndexDef{IndexAlgoParams: "not json"}),
		"unreadable params degrade to no INCLUDE columns rather than erroring")
}

// --- build SQL -------------------------------------------------------------

// The build statement CROSS APPLYs fulltext2_create over the source table with the
// pk and each indexed column.
func TestGenFulltext2BuildFromSourceSQL(t *testing.T) {
	defs := indexDefs("")
	sql, err := genFulltext2BuildFromSourceSQL(
		srcTableDef(), defs[catalog.FullText2Index_TblType_Storage],
		defs[catalog.FullText2Index_TblType_Metadata], "db",
		DefaultMaxIndexCapacity, DefaultMaxPostingsCapacity)
	require.NoError(t, err)
	require.Contains(t, sql, "fulltext2_create(")
	require.Contains(t, sql, "CROSS APPLY")
	require.Contains(t, sql, "`db`.`src`")
	require.Contains(t, sql, "`src`.`body`")
	require.Contains(t, sql, "`src`.`id`")
}

// INCLUDE columns trail the text columns, and their types are recorded in the
// marshalled TableConfig.
func TestGenFulltext2BuildFromSourceSQL_Include(t *testing.T) {
	defs := indexDefs("")
	storeDef := defs[catalog.FullText2Index_TblType_Storage]
	storeDef.IncludedColumns = []string{"payload"}

	sql, err := genFulltext2BuildFromSourceSQL(
		srcTableDef(), storeDef, defs[catalog.FullText2Index_TblType_Metadata], "db",
		DefaultMaxIndexCapacity, DefaultMaxPostingsCapacity)
	require.NoError(t, err)
	require.Contains(t, sql, "`src`.`payload`")
	require.True(t, strings.Index(sql, "`src`.`body`") < strings.Index(sql, "`src`.`payload`"),
		"INCLUDE values trail the text columns")

	// The embedded TableConfig records one INCLUDE type.
	start := strings.Index(sql, `'{`)
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(sql[start:], `}'`) + start + 1
	var cfg struct {
		IncludeTypes []int32 `json:"include_types"`
	}
	require.NoError(t, json.Unmarshal([]byte(sql[start+1:end]), &cfg))
	require.Len(t, cfg.IncludeTypes, 1)
}

// An INCLUDE column absent from the source table is a build error.
func TestGenFulltext2BuildFromSourceSQL_IncludeNotOnSource(t *testing.T) {
	defs := indexDefs("")
	storeDef := defs[catalog.FullText2Index_TblType_Storage]
	storeDef.IncludedColumns = []string{"missing"}

	_, err := genFulltext2BuildFromSourceSQL(
		srcTableDef(), storeDef, defs[catalog.FullText2Index_TblType_Metadata], "db",
		DefaultMaxIndexCapacity, DefaultMaxPostingsCapacity)
	require.Error(t, err)
}

func TestBuildFromSource_NoIndexedColumn(t *testing.T) {
	defs := indexDefs("")
	defs[catalog.FullText2Index_TblType_Storage].Parts = nil
	err := buildFromSource(newStubCtx(), defs[catalog.FullText2Index_TblType_Storage],
		defs[catalog.FullText2Index_TblType_Metadata], srcTableDef(), "db")
	require.Error(t, err)
}

// A malformed capacity errors out of buildFromSource.
func TestBuildFromSource_BadCapacity(t *testing.T) {
	defs := indexDefs(`{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":"abc"}`)
	err := buildFromSource(newStubCtx(), defs[catalog.FullText2Index_TblType_Storage],
		defs[catalog.FullText2Index_TblType_Metadata], srcTableDef(), "db")
	require.Error(t, err)
}

// --- HandleCreateIndex -----------------------------------------------------

// A frontend CREATE builds the base, re-registers the CDC task and registers the
// scheduled compaction.
func TestHandleCreateIndex_OK(t *testing.T) {
	ctx := newStubCtx()
	ctx.isFrontend = true
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, indexDefs("")))
	require.Len(t, ctx.sqls, 1, "one build statement")
	require.Contains(t, ctx.sqls[0], "fulltext2_create(")
	require.Equal(t, 1, ctx.dropped, "the prior CDC task is dropped before recreating")
	require.Equal(t, 1, ctx.created)
	require.Equal(t, 1, ctx.registered)
}

// The experimental gate is frontend-only; a background re-entry skips it.
func TestHandleCreateIndex_ExperimentalGate(t *testing.T) {
	off := newStubCtx()
	off.isFrontend = true
	off.experimental = false
	require.Error(t, Hooks{}.HandleCreateIndex(off, indexDefs("")))

	bg := newStubCtx()
	bg.isFrontend = false
	bg.experimental = false
	require.NoError(t, Hooks{}.HandleCreateIndex(bg, indexDefs("")))
}

func TestHandleCreateIndex_MissingDefs(t *testing.T) {
	for _, missing := range []string{
		catalog.FullText2Index_TblType_Storage,
		catalog.FullText2Index_TblType_Metadata,
	} {
		defs := indexDefs("")
		delete(defs, missing)
		require.Error(t, Hooks{}.HandleCreateIndex(newStubCtx(), defs))
	}
}

// --- HandleReindex ---------------------------------------------------------

// REBUILD (merge=false) clears the tail and the prior bases, then rebuilds.
func TestHandleReindex_Rebuild(t *testing.T) {
	ctx := newStubCtx()
	require.NoError(t, Hooks{}.HandleReindex(ctx, indexDefs(""), false, false))
	require.Greater(t, len(ctx.sqls), 1, "clear statements precede the build")
	require.Contains(t, ctx.sqls[len(ctx.sqls)-1], "fulltext2_create(")
	require.Equal(t, 1, ctx.created)
}

// MERGE folds the tail into the base via fulltext2_compact and leaves the CDC task.
func TestHandleReindex_Merge(t *testing.T) {
	ctx := newStubCtx()
	require.NoError(t, Hooks{}.HandleReindex(ctx, indexDefs(""), false, true))
	require.Len(t, ctx.sqls, 1)
	require.Contains(t, ctx.sqls[0], "fulltext2_compact(")
	require.Equal(t, 0, ctx.created, "a merge does not touch the CDC task")
}

// A merge over an index whose persisted capacity is unreadable errors.
func TestHandleReindex_MergeBadCapacity(t *testing.T) {
	defs := indexDefs(`{"` + catalog.IndexAlgoParamMaxIndexCapacity + `":"abc"}`)
	require.Error(t, Hooks{}.HandleReindex(newStubCtx(), defs, false, true))
}

func TestHandleReindex_MissingDefs(t *testing.T) {
	for _, missing := range []string{
		catalog.FullText2Index_TblType_Storage,
		catalog.FullText2Index_TblType_Metadata,
	} {
		defs := indexDefs("")
		delete(defs, missing)
		require.Error(t, Hooks{}.HandleReindex(newStubCtx(), defs, false, true))
	}
}

// --- the small hooks -------------------------------------------------------

func TestRestoreInitSQL(t *testing.T) {
	ok, sql, err := Hooks{}.RestoreInitSQL(newStubCtx(), nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", sql)
}

func TestHandleDropIndex(t *testing.T) {
	require.NoError(t, Hooks{}.HandleDropIndex(newStubCtx(), nil))
}

// Every hidden-table type drops at priority 0.
func TestHiddenTableDropPriority(t *testing.T) {
	require.Equal(t, 0, Hooks{}.HiddenTableDropPriority(catalog.FullText2Index_TblType_Storage))
	require.Equal(t, 0, Hooks{}.HiddenTableDropPriority(catalog.FullText2Index_TblType_Metadata))
}

// A background re-entry returns a nil blob; a frontend capture returns a non-nil one.
func TestIdxcronMetadata(t *testing.T) {
	bg := newStubCtx()
	bg.isFrontend = false
	blob, err := Hooks{}.IdxcronMetadata(bg)
	require.NoError(t, err)
	require.Nil(t, blob)

	fe := newStubCtx()
	fe.isFrontend = true
	blob, err = Hooks{}.IdxcronMetadata(fe)
	require.NoError(t, err)
	require.NotEmpty(t, blob, "a frontend capture always registers a non-nil blob")
}
