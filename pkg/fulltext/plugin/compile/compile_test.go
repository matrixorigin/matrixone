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

package compile

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// stubCtx is a minimal compileplugin.CompileContext. QryDatabase / OriginalTableDef
// carry values; RunSql, DropIndexCdcTask and CreateIndexCdcTask record their calls so
// handleCreate's build sequence can be asserted. isClone drives the clone branch.
type stubCtx struct {
	qryDatabase string
	tableDef    *plan.TableDef
	isClone     bool
	ccpr        bool

	runSqls          []string
	dropCdcCalled    bool
	createCdcCalled  bool
	createCdcInitSQL string
	createCdcFromNow bool
}

func (s *stubCtx) Ctx() compileplugin.Context             { return nil }
func (s *stubCtx) Database() engine.Database              { return nil }
func (s *stubCtx) QryDatabase() string                    { return s.qryDatabase }
func (s *stubCtx) OriginalTableDef() *plan.TableDef       { return s.tableDef }
func (s *stubCtx) IndexInfo() *plan.CreateTable           { return nil }
func (s *stubCtx) MainTableID() uint64                    { return 0 }
func (s *stubCtx) MainExtra() *api.SchemaExtra            { return nil }
func (s *stubCtx) RunSql(sql string) error                { s.runSqls = append(s.runSqls, sql); return nil }
func (s *stubCtx) BuildIndexTable(_ *plan.TableDef) error { return nil }
func (s *stubCtx) ResolveVariable(_ string, _, _ bool) (any, error) {
	return int64(0), nil
}
func (s *stubCtx) IsFrontend() bool                             { return true }
func (s *stubCtx) IsTableClone() bool                           { return s.isClone }
func (s *stubCtx) IsExperimentalEnabled(_ string) (bool, error) { return true, nil }
func (s *stubCtx) IsCCPRTaskTransaction() bool                  { return s.ccpr }
func (s *stubCtx) IsTableFromPublication(_ *plan.TableDef) bool { return s.ccpr }
func (s *stubCtx) SinkerTypeFromAlgo(_ string) int8             { return 0 }
func (s *stubCtx) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, startFromNow bool, sql string, _ *plan.TableDef) error {
	s.createCdcCalled = true
	s.createCdcFromNow = startFromNow
	s.createCdcInitSQL = sql
	return nil
}
func (s *stubCtx) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	s.dropCdcCalled = true
	return nil
}
func (s *stubCtx) RunSqlWithResult(_ string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (s *stubCtx) RegisterIdxcronUpdate(_ uint64, _, _, _, _ string, _ []byte) error { return nil }

func retrievalDefs() map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		"": {
			IndexName:       "ftidx",
			IndexTableName:  "postings_tbl",
			IndexAlgoParams: retrievalParams,
			Parts:           []string{"body"},
		},
		catalog.FullTextIndex_TblType_Storage:  {IndexTableName: "store_tbl"},
		catalog.FullTextIndex_TblType_Metadata: {IndexTableName: "meta_tbl"},
	}
}

func retrievalCtx(isClone bool) *stubCtx {
	return &stubCtx{
		qryDatabase: "db1",
		tableDef:    &plan.TableDef{Name: "t1", Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}},
		isClone:     isClone,
	}
}

const retrievalParams = `{"parser":"retrieval"}`
const ngramParams = `{"parser":"ngram"}`

// --- RestoreInitSQL: the clone/restore rebuild seam (commit 3edae8b5c) ---

// A retrieval index rebuilds its tag=0 base post-clone via ALTER … REINDEX …
// FULLTEXT FORCE_SYNC, so the block-clone's seed+copy duplicate is discarded.
func TestRestoreInitSQL_Retrieval_ReindexSQL(t *testing.T) {
	ctx := &stubCtx{qryDatabase: "db1", tableDef: &plan.TableDef{Name: "t1"}}
	defs := map[string]*plan.IndexDef{
		"": {IndexName: "ftidx", IndexAlgoParams: retrievalParams},
	}
	startFromNow, sql, err := Hooks{}.RestoreInitSQL(ctx, defs)
	require.NoError(t, err)
	require.True(t, startFromNow)
	require.Equal(t, "ALTER TABLE `db1`.`t1` ALTER REINDEX `ftidx` FULLTEXT FORCE_SYNC", sql)
}

// A postings/ngram index has no compact model: it must return a NON-empty no-op
// so RestoreTable keeps startFromNow=true and the CDC doesn't replay cloned rows.
func TestRestoreInitSQL_NonRetrieval_SelectOne(t *testing.T) {
	ctx := &stubCtx{qryDatabase: "db1", tableDef: &plan.TableDef{Name: "t1"}}
	for _, params := range []string{ngramParams, `{}`, ""} {
		defs := map[string]*plan.IndexDef{"": {IndexName: "ftidx", IndexAlgoParams: params}}
		startFromNow, sql, err := Hooks{}.RestoreInitSQL(ctx, defs)
		require.NoError(t, err)
		require.True(t, startFromNow, "params=%q", params)
		require.Equal(t, "SELECT 1", sql, "params=%q", params)
	}
}

func TestRestoreInitSQL_MissingPostingsDef_Errors(t *testing.T) {
	ctx := &stubCtx{qryDatabase: "db1", tableDef: &plan.TableDef{Name: "t1"}}
	_, _, err := Hooks{}.RestoreInitSQL(ctx, map[string]*plan.IndexDef{})
	require.Error(t, err)
}

// --- HandleReindex gate: only retrieval is rebuildable ---

// MERGE (incremental WAND compaction) is retrieval-only — rejected on a non-retrieval
// (ngram) index with a MERGE-specific message so the user knows to use a plain REBUILD.
func TestHandleReindex_NonRetrievalMerge_NotSupported(t *testing.T) {
	ctx := &stubCtx{qryDatabase: "db1", tableDef: &plan.TableDef{Name: "t1"}}
	defs := map[string]*plan.IndexDef{"": {IndexName: "ftidx", IndexAlgoParams: ngramParams}}
	err := Hooks{}.HandleReindex(ctx, defs, false, true)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not supported")
	require.Contains(t, err.Error(), "MERGE")
}

// Plain REBUILD (merge=false) works for a non-retrieval (ngram) index: it clears the
// postings table and re-tokenizes from source (no CDC, no WAND store) — the same 3c path
// as CREATE, now idempotent.
func TestHandleReindex_NonRetrievalRebuild_ClearsAndRepopulates(t *testing.T) {
	ctx := retrievalCtx(false)
	defs := map[string]*plan.IndexDef{
		"": {IndexName: "ftidx", IndexTableName: "postings_tbl", IndexAlgoParams: ngramParams, Parts: []string{"body"}},
	}
	require.NoError(t, Hooks{}.HandleReindex(ctx, defs, false, false))
	require.False(t, ctx.createCdcCalled, "ngram rebuild registers no CDC")
	require.Len(t, ctx.runSqls, 2)
	require.Contains(t, ctx.runSqls[0], "DELETE FROM")
	require.Contains(t, ctx.runSqls[1], "fulltext_index_tokenize")
}

func TestHandleReindex_MissingPostingsDef_Errors(t *testing.T) {
	ctx := &stubCtx{qryDatabase: "db1", tableDef: &plan.TableDef{Name: "t1"}}
	err := Hooks{}.HandleReindex(ctx, map[string]*plan.IndexDef{}, false, false)
	require.Error(t, err)
}

// --- handleCreate branch selection ---

// Retrieval sync build (the default): drops any prior CDC, clears postings + tail,
// runs the postings INSERT + tag=0 wand build inline, then registers CDC from NOW
// with an empty InitSQL (the inline build already produced the base).
func TestHandleCreateIndex_RetrievalSync_BuildsAndRegistersCDC(t *testing.T) {
	ctx := retrievalCtx(false)
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, retrievalDefs()))

	require.True(t, ctx.dropCdcCalled)
	require.True(t, ctx.createCdcCalled)
	require.True(t, ctx.createCdcFromNow, "sync build registers CDC startFromNow=true")
	require.Equal(t, "", ctx.createCdcInitSQL, "sync build needs no InitSQL")

	joined := strings.Join(ctx.runSqls, "\n")
	require.Contains(t, joined, "DELETE FROM", "clears old postings for REINDEX idempotency")
	require.Contains(t, joined, "fulltext_wand_create", "builds the tag=0 base inline")
}

// Async retrieval: the initial build is deferred to CDC — register a task from ts=0
// with a non-empty InitSQL (postings INSERT + tag=0 build) run at task start.
func TestHandleCreateIndex_AsyncRetrieval_RegistersCDCWithInitSQL(t *testing.T) {
	ctx := retrievalCtx(false)
	defs := retrievalDefs()
	defs[""].IndexAlgoParams = `{"parser":"retrieval","async":"true"}`

	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, defs))

	require.True(t, ctx.createCdcCalled)
	require.False(t, ctx.createCdcFromNow, "async build consumes the full log from ts=0")
	require.Contains(t, ctx.createCdcInitSQL, "fulltext_wand_create", "InitSQL builds tag=0 at task start")
	require.Empty(t, ctx.runSqls, "async build runs nothing inline")
}

// CCPR task transaction on a publication-subscribed table: skip data population
// entirely (the index data syncs via the CCPR pipeline instead).
func TestHandleCreateIndex_CCPRSkip(t *testing.T) {
	ctx := retrievalCtx(false)
	ctx.ccpr = true
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, retrievalDefs()))
	require.False(t, ctx.createCdcCalled)
	require.Empty(t, ctx.runSqls)
}

// Non-retrieval sync (default parser): populate the single postings hidden table
// inline via fulltext_index_tokenize; no WAND store, no CDC task.
func TestHandleCreateIndex_NonRetrievalSync_PopulatesPostingsOnly(t *testing.T) {
	ctx := retrievalCtx(false)
	defs := map[string]*plan.IndexDef{
		"": {
			IndexName:       "ftidx",
			IndexTableName:  "postings_tbl",
			IndexAlgoParams: ngramParams,
			Parts:           []string{"body"},
		},
	}
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, defs))

	require.False(t, ctx.createCdcCalled, "sync non-retrieval registers no CDC")
	// clear-then-repopulate (idempotent so ALTER … REINDEX rebuilds); the DELETE is a
	// no-op on a fresh CREATE but keeps the path reusable for reindex.
	require.Len(t, ctx.runSqls, 2)
	require.Contains(t, ctx.runSqls[0], "DELETE FROM")
	require.Contains(t, ctx.runSqls[1], "fulltext_index_tokenize")
}

// --- pure SQL builders ---

// genWandBuildSQL builds the WAND store from the postings table via CROSS APPLY
// fulltext_wand_create, threading the store + metadata table names through cfg.
func TestGenWandBuildSQL(t *testing.T) {
	postingsDef := &plan.IndexDef{IndexTableName: "postings_tbl", IndexAlgoParams: retrievalParams}
	storeDef := &plan.IndexDef{IndexTableName: "store_tbl"}
	metaDef := &plan.IndexDef{IndexTableName: "meta_tbl"}

	sqls, err := genWandBuildSQL(postingsDef, storeDef, metaDef, "db1", 1000)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	sql := sqls[0]
	require.Contains(t, sql, "fulltext_wand_create")
	require.Contains(t, sql, "`db1`.`postings_tbl`")
	// the store/metadata table names ride in the JSON cfg arg
	require.Contains(t, sql, "store_tbl")
	require.Contains(t, sql, "meta_tbl")
	require.Contains(t, sql, catalog.FullTextIndex_TabCol_Word)
	require.Contains(t, sql, catalog.FullTextIndex_TabCol_Id)
}

// wandInitSQL (async CREATE InitSQL) is a JSON array: postings INSERT(s) then the
// tag=0 build. Returns "" when there is no WAND store (postings/ngram index).
func TestWandInitSQL(t *testing.T) {
	tableDef := &plan.TableDef{
		Name: "t1",
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	indexDef := &plan.IndexDef{
		IndexName:       "ftidx",
		IndexTableName:  "postings_tbl",
		IndexAlgoParams: retrievalParams,
		Parts:           []string{"body"},
	}
	defs := map[string]*plan.IndexDef{
		"":                                     indexDef,
		catalog.FullTextIndex_TblType_Storage:  {IndexTableName: "store_tbl"},
		catalog.FullTextIndex_TblType_Metadata: {IndexTableName: "meta_tbl"},
	}
	js, err := wandInitSQL(defs, tableDef, indexDef, "db1", 1000)
	require.NoError(t, err)

	var stmts []string
	require.NoError(t, json.Unmarshal([]byte(js), &stmts))
	require.GreaterOrEqual(t, len(stmts), 2) // >=1 postings insert + 1 build
	require.Contains(t, stmts[len(stmts)-1], "fulltext_wand_create")

	// no WAND store def -> "" (non-retrieval / postings-only index)
	js2, err := wandInitSQL(map[string]*plan.IndexDef{}, tableDef, indexDef, "db1", 1000)
	require.NoError(t, err)
	require.Equal(t, "", js2)
}

// --- cheap coverage for the trivial hooks ---

func TestValidateReindexParams_Passthrough(t *testing.T) {
	old := map[string]string{"a": "1"}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{})
	require.NoError(t, err)
	require.Equal(t, old, got)
}

// IdxcronMetadata captures fulltext_max_index_capacity so the background idxcron
// reindex rebuilds with the right capacity. The stub ctx is a frontend session with a
// resolvable capacity var, so the blob is non-empty (and carries the captured var).
func TestIdxcronMetadata_CapturesCapacity(t *testing.T) {
	got, err := Hooks{}.IdxcronMetadata(&stubCtx{})
	require.NoError(t, err)
	require.NotEmpty(t, got)
	require.Contains(t, string(got), "fulltext_max_index_capacity")
}

// A postings/ngram index has no Storage def, so HandleDropIndex is a no-op.
func TestHandleDropIndex_NonRetrieval_NoOp(t *testing.T) {
	defs := map[string]*plan.IndexDef{"": {IndexName: "ftidx", IndexAlgoParams: ngramParams}}
	require.NoError(t, Hooks{}.HandleDropIndex(&stubCtx{}, defs))
}

// ValidateReindexParams accepts a new max_index_capacity on ALTER … REINDEX (merged so the
// rebuild repartitions the base), rejects any other option, and keeps the stored value when
// the statement carries none (e.g. the idxcron rebuild).
func TestValidateReindexParams_MaxIndexCapacity(t *testing.T) {
	old := map[string]string{
		catalog.IndexAlgoParamParser:           "retrieval",
		catalog.IndexAlgoParamMaxIndexCapacity: "2",
	}
	// accept: max_index_capacity is merged
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.IndexAlgoParamMaxIndexCapacity: "8"},
	})
	require.NoError(t, err)
	require.Equal(t, "8", got[catalog.IndexAlgoParamMaxIndexCapacity])
	// reject: any other REINDEX option
	_, err = Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.IndexAlgoParamLists: "5"},
	})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not supported")
	// omitted: the stored value is kept
	got, err = Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{})
	require.NoError(t, err)
	require.Equal(t, "2", got[catalog.IndexAlgoParamMaxIndexCapacity])
}
