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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

// scriptedCtx extends the smoke test's stubCtx with the two things the
// create/reindex body actually needs from its context: a record of every SQL it
// ran (so the generated entry projection can be asserted on) and scripted
// results for the metadata reads that readQuantizeBound performs.
type scriptedCtx struct {
	stubCtx

	db     string
	tblDef *plan.TableDef

	mu   sync.Mutex
	sqls []string

	// failSQLSubstr makes RunSql fail for the first statement containing this
	// substring — used to prove the build aborts (and propagates) instead of
	// continuing on to prune/registration steps.
	failSQLSubstr string

	// failSelectSubstr makes RunSqlWithResult fail for a SELECT containing this
	// substring, i.e. a metadata read error.
	failSelectSubstr string

	// bounds maps a metadata key (quantize_min / quantize_max) to the scripted
	// result of its lookup. A missing key yields an empty result, which is how a
	// pre-quantizer index (no bounds row) looks.
	bounds map[string]executor.Result
}

func (c *scriptedCtx) QryDatabase() string              { return c.db }
func (c *scriptedCtx) OriginalTableDef() *plan.TableDef { return c.tblDef }

func (c *scriptedCtx) RunSql(sql string) error {
	c.mu.Lock()
	c.sqls = append(c.sqls, sql)
	c.mu.Unlock()
	if c.failSQLSubstr != "" && strings.Contains(sql, c.failSQLSubstr) {
		return moerr.NewInternalErrorNoCtx("scripted RunSql failure")
	}
	return nil
}

func (c *scriptedCtx) RunSqlWithResult(sql string) (executor.Result, error) {
	c.mu.Lock()
	c.sqls = append(c.sqls, sql)
	c.mu.Unlock()
	if c.failSelectSubstr != "" && strings.Contains(sql, c.failSelectSubstr) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("scripted SELECT failure")
	}
	for key, res := range c.bounds {
		if strings.Contains(sql, "'"+key+"'") {
			return res, nil
		}
	}
	// count(*) and any unscripted lookup: no batches at all.
	return executor.Result{}, nil
}

// joinSQL returns the single INSERT ... CENTROIDX ... statement produced by
// ivfIndexEntriesTable (the one carrying the entry projection).
func (c *scriptedCtx) joinSQL(t *testing.T) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var found string
	for _, s := range c.sqls {
		if strings.Contains(s, "CENTROIDX") {
			require.Empty(t, found, "expected exactly one CENTROIDX insert")
			found = s
		}
	}
	require.NotEmpty(t, found, "no CENTROIDX insert was run; sqls=%v", c.sqls)
	return found
}

// cacheSkipCtx is scriptedCtx plus the optional RunWithSourceReadCacheSkip
// interface. runCreateOrReindex type-asserts for it, so the two build dispatch
// branches need two distinct context types.
type cacheSkipCtx struct {
	scriptedCtx
	skipCalls int
}

func (c *cacheSkipCtx) RunWithSourceReadCacheSkip(fn func() error) error {
	c.skipCalls++
	return fn()
}

// doubleResult builds the shape RunSqlWithResult returns for
// `SELECT CAST(val AS DOUBLE) ...`: one batch, one DOUBLE column.
func doubleResult(t *testing.T, vals ...float64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_float64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
}

// noColsResult is a batch that carries no column at all. A real engine can
// return this for a metadata SELECT that matched no row, and the len(cols)==0
// guard is the only thing between it and an index-out-of-range panic in DDL.
func noColsResult(t *testing.T) executor.Result {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(0)
	bat.SetRowCount(0)
	return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
}

func srcTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "src",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 8}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
}

func entriesDef(algoParams string) *plan.IndexDef {
	return &plan.IndexDef{
		IndexName:       "idx",
		IndexTableName:  "__mo_index_entries",
		Parts:           []string{"embedding"},
		IndexAlgoParams: algoParams,
	}
}

func params(kv ...string) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:%q", kv[i], kv[i+1])
	}
	sb.WriteByte('}')
	return sb.String()
}

// ---------------------------------------------------------------------------
// entry projection under QUANTIZATION
// ---------------------------------------------------------------------------

// TestIvfEntriesTable_QuantizedEntryProjection pins the SELECT expression the
// ENTRIES insert projects for each QUANTIZATION value. This is the one place the
// build side derives q(x)=x*mul+add from the trained bounds; if it drifts from
// quantizer.Int8Params/Uint8Params the stored entries no longer live in the same
// space as the quantized query vector and recall collapses silently (no error,
// just wrong neighbours) — which is exactly the class of bug the narrow-type work
// hit before.
func TestIvfEntriesTable_QuantizedEntryProjection(t *testing.T) {
	const (
		qmin = -0.75
		qmax = 1.25
		dim  = 8
	)
	int8Mul, int8Add := quantizer.Int8Params(qmin, qmax)
	u8Mul, u8Add := quantizer.Uint8Params(qmin, qmax)

	cases := []struct {
		name string
		// quant is the QUANTIZATION algo-param value ("" omits the key entirely).
		quant string
		// withBounds controls whether the metadata table has the trained
		// quantize_min / quantize_max rows.
		withBounds bool
		wantExpr   string
	}{
		{
			// int8 with trained bounds: the full affine transform must be inlined.
			name: "int8 with trained bounds", quant: "int8", withBounds: true,
			wantExpr: quantizer.Int8EntrySQL("`embedding`", int8Mul, int8Add, dim),
		},
		{
			// uint8 uses the same bounds but the un-shifted params — reusing the
			// int8 add (-128) here would push every value out of [0,255].
			name: "uint8 with trained bounds", quant: "uint8", withBounds: true,
			wantExpr: quantizer.Uint8EntrySQL("`embedding`", u8Mul, u8Add, dim),
		},
		{
			// Pre-quantizer index (metadata has no bounds row): must degrade to a
			// raw cast rather than emitting a NaN/garbage scale.
			name: "int8 without trained bounds", quant: "int8", withBounds: false,
			wantExpr: quantizer.CastSQL("`embedding`", types.T_array_int8, dim),
		},
		{
			name: "uint8 without trained bounds", quant: "uint8", withBounds: false,
			wantExpr: quantizer.CastSQL("`embedding`", types.T_array_uint8, dim),
		},
		{
			// float formats never read bounds — a plain narrowing cast.
			name: "float16", quant: "float16", withBounds: false,
			wantExpr: quantizer.CastSQL("`embedding`", types.T_array_float16, dim),
		},
		{
			name: "bf16", quant: "bf16", withBounds: false,
			wantExpr: quantizer.CastSQL("`embedding`", types.T_array_bf16, dim),
		},
		{
			// No QUANTIZATION: entries keep the base column verbatim.
			name: "unquantized", quant: "",
			wantExpr: "`embedding`",
		},
		{
			// An unrecognised value must not be spliced into SQL; entries fall back
			// to the base column (ToVectorType returns ok=false).
			name: "unknown quantization", quant: "garbage",
			wantExpr: "`embedding`",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv := []string{catalog.IndexAlgoParamOpType, "vector_l2_ops"}
			if tc.quant != "" {
				kv = append(kv, catalog.Quantization, tc.quant)
			}
			ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
			if tc.withBounds {
				ctx.bounds = map[string]executor.Result{
					catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin: doubleResult(t, qmin),
					catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax: doubleResult(t, qmax),
				}
			}

			err := ivfIndexEntriesTable(ctx, entriesDef(params(kv...)), "db",
				srcTableDef(), "__mo_index_meta", "__mo_index_centroids")
			require.NoError(t, err)

			sql := ctx.joinSQL(t)
			require.Contains(t, sql, tc.wantExpr)
			// The insert target must be the ENTRIES table (not centroids/meta) and
			// carry the four entry columns in order.
			require.Contains(t, sql, "insert into `db`.`__mo_index_entries` (`"+
				catalog.SystemSI_IVFFLAT_TblCol_Entries_version+"`, `"+
				catalog.SystemSI_IVFFLAT_TblCol_Entries_id+"`, `"+
				catalog.SystemSI_IVFFLAT_TblCol_Entries_pk+"`, `"+
				catalog.SystemSI_IVFFLAT_TblCol_Entries_entry+"`)")
			// op_type must reach CENTROIDX; the centroid assignment always runs on
			// the f32 base column, never on the quantized expression.
			require.Contains(t, sql, "CENTROIDX ('vector_l2_ops')")
		})
	}
}

// TestIvfEntriesTable_PartialBoundsFallBackToCast: only one of the two bounds
// present is not a usable range — deriving mul from a missing max would produce a
// nonsense scale, so the build must fall back to the raw cast. Also drives the
// "row present but no column"/"column present but no row" result shapes, which are
// what a metadata SELECT returns when the key is absent.
func TestIvfEntriesTable_PartialBoundsFallBackToCast(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bounds map[string]executor.Result
	}{
		{
			name: "max missing (no rows)",
			bounds: map[string]executor.Result{
				catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin: doubleResult(t, -1),
				catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax: doubleResult(t), // zero rows
			},
		},
		{
			name: "min missing (batch has no column)",
			bounds: map[string]executor.Result{
				catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin: noColsResult(t),
				catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax: doubleResult(t, 1),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &scriptedCtx{db: "db", tblDef: srcTableDef(), bounds: tc.bounds}
			err := ivfIndexEntriesTable(ctx, entriesDef(params(
				catalog.IndexAlgoParamOpType, "vector_l2_ops",
				catalog.Quantization, "int8")), "db", srcTableDef(),
				"__mo_index_meta", "__mo_index_centroids")
			require.NoError(t, err)
			sql := ctx.joinSQL(t)
			require.Contains(t, sql, quantizer.CastSQL("`embedding`", types.T_array_int8, 8))
			require.NotContains(t, sql, "NaN")
		})
	}
}

// TestIvfEntriesTable_BoundReadErrorPropagates: a failed metadata read must abort
// the DDL. Swallowing it would build the index with an identity-cast projection
// while search applies the trained transform — a silent recall bug rather than a
// visible failure.
func TestIvfEntriesTable_BoundReadErrorPropagates(t *testing.T) {
	for _, key := range []string{
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin,
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax,
	} {
		t.Run(key, func(t *testing.T) {
			ctx := &scriptedCtx{
				db: "db", tblDef: srcTableDef(),
				failSelectSubstr: "'" + key + "'",
				bounds: map[string]executor.Result{
					catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin: doubleResult(t, -1),
					catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax: doubleResult(t, 1),
				},
			}
			err := ivfIndexEntriesTable(ctx, entriesDef(params(
				catalog.IndexAlgoParamOpType, "vector_l2_ops",
				catalog.Quantization, "int8")), "db", srcTableDef(),
				"__mo_index_meta", "__mo_index_centroids")
			require.Error(t, err)
			// The insert must not have been attempted after the failed read.
			ctx.mu.Lock()
			defer ctx.mu.Unlock()
			for _, s := range ctx.sqls {
				require.NotContains(t, s, "CENTROIDX")
			}
		})
	}
}

// TestIvfEntriesTable_CompositePrimaryKey: with a composite PK the entry rows key
// on serial(...) of the parts, not on the hidden __mo_cpkey_col — getting this
// wrong makes the entries unjoinable back to the source table at re-rank time.
func TestIvfEntriesTable_CompositePrimaryKey(t *testing.T) {
	tbl := srcTableDef()
	tbl.Pkey = &plan.PrimaryKeyDef{
		PkeyColName: catalog.CPrimaryKeyColName,
		Names:       []string{"a", "b"},
	}
	ctx := &scriptedCtx{db: "db", tblDef: tbl}
	require.NoError(t, ivfIndexEntriesTable(ctx, entriesDef(params(
		catalog.IndexAlgoParamOpType, "vector_l2_ops")), "db", tbl,
		"__mo_index_meta", "__mo_index_centroids"))
	require.Contains(t, ctx.joinSQL(t), "serial(`src`.`a`,`src`.`b`)")
}

// TestReadQuantizeBound_Shapes exercises readQuantizeBound directly for the
// result shapes ivfIndexEntriesTable can hand it, including the found/not-found
// distinction the int8 path branches on.
func TestReadQuantizeBound_Shapes(t *testing.T) {
	key := catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin

	t.Run("found", func(t *testing.T) {
		ctx := &scriptedCtx{db: "db", tblDef: srcTableDef(), bounds: map[string]executor.Result{
			key: doubleResult(t, 3.5, 9.5), // only the first row is meaningful
		}}
		val, found, err := readQuantizeBound(ctx, "db", "__mo_index_meta", key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 3.5, val)
		// The lookup must be scoped to the requested key, otherwise min and max
		// would read the same row.
		require.Contains(t, ctx.sqls[0], "'"+key+"'")
	})

	t.Run("no batches", func(t *testing.T) {
		ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
		val, found, err := readQuantizeBound(ctx, "db", "__mo_index_meta", key)
		require.NoError(t, err)
		require.False(t, found)
		require.Zero(t, val)
	})

	t.Run("error", func(t *testing.T) {
		ctx := &scriptedCtx{db: "db", tblDef: srcTableDef(), failSelectSubstr: key}
		_, found, err := readQuantizeBound(ctx, "db", "__mo_index_meta", key)
		require.Error(t, err)
		require.False(t, found)
	})
}

// ---------------------------------------------------------------------------
// create / reindex body
// ---------------------------------------------------------------------------

func ivfIndexDefs(algoParams string) map[string]*plan.IndexDef {
	mk := func(tbl string) *plan.IndexDef {
		return &plan.IndexDef{
			IndexName:       "idx",
			IndexTableName:  tbl,
			Parts:           []string{"embedding"},
			IndexAlgoParams: algoParams,
		}
	}
	return map[string]*plan.IndexDef{
		catalog.SystemSI_IVFFLAT_TblType_Metadata:  mk("__mo_index_meta"),
		catalog.SystemSI_IVFFLAT_TblType_Centroids: mk("__mo_index_centroids"),
		catalog.SystemSI_IVFFLAT_TblType_Entries:   mk("__mo_index_entries"),
	}
}

// fakeCachedIndex is a cache-resident index whose Destroy() only records that it
// was evicted.
type fakeCachedIndex struct{ destroyed atomic.Bool }

func (f *fakeCachedIndex) Search(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, nil
}
func (f *fakeCachedIndex) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ []int64, _ []float32) error {
	return nil
}
func (f *fakeCachedIndex) Load(_ *sqlexec.SqlProcess) error               { return nil }
func (f *fakeCachedIndex) UpdateConfig(_ cache.VectorIndexSearchIf) error { return nil }
func (f *fakeCachedIndex) Destroy()                                       { f.destroyed.Store(true) }

// seedCache parks a fake index under the given cache key and returns it.
func seedCache(key string) *fakeCachedIndex {
	algo := &fakeCachedIndex{}
	s := &cache.VectorIndexSearch{Algo: algo}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	cache.Cache.IndexMap.Store(key, s)
	return algo
}

// TestRunCreateOrReindex_EvictsEveryCachedGeneration: the search cache key is
// "<centroidsTable>:<version>" (plus a /cnIdx/cnCnt suffix when the scan is split
// across CNs), and the version comes from the meta table. A rebuild bumps that
// version, so an eviction that guessed ":0" left the live entry — every
// generation of THIS index must go, and no other index's entries may be touched.
func TestRunCreateOrReindex_EvictsEveryCachedGeneration(t *testing.T) {
	const centroids = "__mo_index_centroids_evict"
	gen0 := seedCache(centroids + ":0")
	gen7 := seedCache(centroids + ":7")
	split := seedCache(centroids + ":7/1/4")
	other := seedCache("__mo_other_centroids:0")
	defer cache.Cache.Remove("__mo_other_centroids:0")

	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops"))
	defs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName = centroids

	ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	require.NoError(t, runCreateOrReindex(ctx, defs, false))

	require.True(t, gen0.destroyed.Load())
	require.True(t, gen7.destroyed.Load(), "a rebuilt (version>0) index must be evicted")
	require.True(t, split.destroyed.Load(), "the split-scan cache key must be evicted")
	require.False(t, other.destroyed.Load(), "eviction must not spill to another index")
}

// TestRunCreateOrReindex_SyncBuildRunsEntries: the synchronous (non-async) path
// must run all four steps in order — meta version bump, centroid build, entry
// assignment, then pruning of the superseded generation. Skipping the prune would
// leave old-version entries that searches at the new version silently ignore, and
// skipping the entries insert leaves an index that returns nothing.
func TestRunCreateOrReindex_SyncBuildRunsEntries(t *testing.T) {
	ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops",
		catalog.Quantization, "int8"))
	ctx.bounds = map[string]executor.Result{
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin: doubleResult(t, -2),
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax: doubleResult(t, 2),
	}

	require.NoError(t, runCreateOrReindex(ctx, defs, false))

	joined := strings.Join(ctx.sqls, "\n")
	require.Contains(t, joined, "insert into `db`.`__mo_index_meta`")
	require.Contains(t, joined, "INSERT INTO `db`.`__mo_index_centroids`")
	require.Contains(t, joined, "CENTROIDX")
	require.Contains(t, joined, "DELETE FROM `db`.`__mo_index_centroids`")
	require.Contains(t, joined, "DELETE FROM `db`.`__mo_index_entries`")
	// The quantization declared on the index must reach the entry projection
	// through the whole create path, not just when called directly.
	mul, add := quantizer.Int8Params(-2, 2)
	require.Contains(t, joined, quantizer.Int8EntrySQL("`embedding`", mul, add, 8))
}

// TestRunCreateOrReindex_AsyncDefersEntries: for an async index the entry
// assignment is the CDC pipeline's job (it replays from ts=0). Running it inline
// here would double-write every entry.
func TestRunCreateOrReindex_AsyncDefersEntries(t *testing.T) {
	ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops",
		catalog.Async, "true"))

	require.NoError(t, runCreateOrReindex(ctx, defs, false))
	require.NotContains(t, strings.Join(ctx.sqls, "\n"), "CENTROIDX")
}

// TestRunCreateOrReindex_AsyncForceSyncRunsEntries: ALTER ... REINDEX FORCE_SYNC
// (and the idxcron-issued rebuild) must build inside the txn even for an async
// index, otherwise the reindex returns with the old centroids still live.
func TestRunCreateOrReindex_AsyncForceSyncRunsEntries(t *testing.T) {
	ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops",
		catalog.Async, "true"))

	require.NoError(t, runCreateOrReindex(ctx, defs, true))
	require.Contains(t, strings.Join(ctx.sqls, "\n"), "CENTROIDX")
}

// TestRunCreateOrReindex_UsesSourceReadCacheSkip: the build's one-shot source
// scan must run under RunWithSourceReadCacheSkip when the context offers it, so
// it doesn't evict the index working set from the fileservice cache. The
// optional-interface must also stay optional — a context without the method still
// builds.
func TestRunCreateOrReindex_UsesSourceReadCacheSkip(t *testing.T) {
	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops"))

	skipping := &cacheSkipCtx{scriptedCtx: scriptedCtx{db: "db", tblDef: srcTableDef()}}
	require.NoError(t, runCreateOrReindex(skipping, defs, false))
	require.Equal(t, 1, skipping.skipCalls)
	require.Contains(t, strings.Join(skipping.sqls, "\n"), "CENTROIDX")

	plainCtx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	require.NoError(t, runCreateOrReindex(plainCtx, defs, false))
	require.Contains(t, strings.Join(plainCtx.sqls, "\n"), "CENTROIDX")
}

// TestRunCreateOrReindex_BuildFailureAborts: a failure inside the build closure
// must propagate out of the cache-skip wrapper and stop the DDL before pruning.
// Pruning after a failed build would delete the previous generation's entries and
// leave the index empty.
func TestRunCreateOrReindex_BuildFailureAborts(t *testing.T) {
	defs := ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops"))

	for _, tc := range []struct {
		name string
		fail string
	}{
		{"centroid build fails", "INSERT INTO `db`.`__mo_index_centroids`"},
		{"entry assignment fails", "CENTROIDX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Both dispatch branches must propagate identically.
			skipping := &cacheSkipCtx{scriptedCtx: scriptedCtx{
				db: "db", tblDef: srcTableDef(), failSQLSubstr: tc.fail}}
			require.Error(t, runCreateOrReindex(skipping, defs, false))
			require.NotContains(t, strings.Join(skipping.sqls, "\n"), "DELETE FROM")

			plainCtx := &scriptedCtx{db: "db", tblDef: srcTableDef(), failSQLSubstr: tc.fail}
			require.Error(t, runCreateOrReindex(plainCtx, defs, false))
			require.NotContains(t, strings.Join(plainCtx.sqls, "\n"), "DELETE FROM")
		})
	}
}

// TestValidateReindexParams_EmptyOpTypeIsUnchanged guards a regression that has
// already bitten once: ALTER ... REINDEX carries only the params being changed,
// so an absent op_type means "unchanged" (it was validated at CREATE). Rejecting
// it here breaks `alter table ... alter reindex ... quantization='int8'`, which
// sends no op_type at all.
func TestValidateReindexParams_EmptyOpTypeIsUnchanged(t *testing.T) {
	for _, quant := range []string{"int8", "uint8"} {
		got, err := Hooks{}.ValidateReindexParams(
			map[string]string{catalog.IndexAlgoParamLists: "16"},
			compileplugin.ReindexParamUpdate{Params: map[string]string{catalog.Quantization: quant}},
		)
		require.NoError(t, err, "empty op_type must mean unchanged, not invalid")
		require.Equal(t, quant, got[catalog.Quantization])
		require.Equal(t, "16", got[catalog.IndexAlgoParamLists], "untouched params must survive the merge")
	}

	// A non-L2 op_type stays rejected for narrow quantization: the quantized-score
	// rescale assumes L2 geometry.
	_, err := Hooks{}.ValidateReindexParams(nil, compileplugin.ReindexParamUpdate{
		Params: map[string]string{
			catalog.Quantization:         "int8",
			catalog.IndexAlgoParamOpType: "vector_l1_ops",
		},
	})
	require.Error(t, err)
}
