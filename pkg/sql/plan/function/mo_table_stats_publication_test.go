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

package function

import (
	"context"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfopb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The loss boundary of issues #30910/#31164: a missing statistics object, an
// unpublished column, or a non-finite estimate must render NULL so an
// unavailable estimate is never confused with the known zero that ANALYZE
// publishes for an empty analyzed table.
func TestNdvEstimateFromStatsInfoUnavailableRendersNull(t *testing.T) {
	require.Nil(t, ndvEstimateFromStatsInfo(nil, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(&statsinfopb.StatsInfo{}, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(
		&statsinfopb.StatsInfo{NdvMap: map[string]float64{"other": 8}}, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(
		&statsinfopb.StatsInfo{NdvMap: map[string]float64{"a": math.NaN()}}, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(
		&statsinfopb.StatsInfo{NdvMap: map[string]float64{"a": math.Inf(-1)}}, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(
		&statsinfopb.StatsInfo{NdvMap: map[string]float64{"a": math.Inf(1)}}, "a"))
	require.Nil(t, ndvEstimateFromStatsInfo(
		&statsinfopb.StatsInfo{NdvMap: map[string]float64{"a": -0.5}, TableCnt: math.NaN()}, "a"))
}

// TestNdvEstimateFromStatsInfoResolvesDeclaredSpelling pins the mixed-case
// review finding (#30910): mo_indexes and mo_columns carry a column's declared
// spelling while the statistics source publishes the canonical lowercased key,
// so the supplied name must resolve to the canonical key before the lookup.
// Display spelling stays untouched: only the lookup resolves the name.
func TestNdvEstimateFromStatsInfoResolvesDeclaredSpelling(t *testing.T) {
	published := &statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"id": 3, "v": 3},
		TableCnt: 3,
	}
	require.EqualValues(t, 3, *ndvEstimateFromStatsInfo(published, "ID"))
	require.EqualValues(t, 3, *ndvEstimateFromStatsInfo(published, "V"))
	require.EqualValues(t, 3, *ndvEstimateFromStatsInfo(published, "id"))
	require.EqualValues(t, 3, *ndvEstimateFromStatsInfo(published, "v"))
	// a lowercased estimate never masquerades as published for another column
	require.Nil(t, ndvEstimateFromStatsInfo(published, "Missing"))
	// the synthetic rowid column has no published NDV even on an analyzed
	// table, so its estimate is unavailable: NULL, never an error and never
	// a false zero
	require.Nil(t, ndvEstimateFromStatsInfo(published, "__mo_rowid"))
}

// TestIndexPartPrefixTruncatesColumn pins the character-prefix review finding
// (#30910): a KEY part configured as col(4) describes LEFT(col, 4), whose
// population the engine's statistics do not estimate, so the lookup must
// refuse the full-column substitution instead of publishing it. Metadata that
// cannot be decoded counts as unavailable rather than guessed.
func TestIndexPartPrefixTruncatesColumn(t *testing.T) {
	require.False(t, indexPartPrefixTruncatesColumn("", "v"))
	require.False(t, indexPartPrefixTruncatesColumn("", "V"))
	// ordinary non-prefix algorithm parameters keep the full-column estimate
	require.False(t, indexPartPrefixTruncatesColumn(`{"lists":"10"}`, "v"))
	require.False(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":""}`, "v"))
	// a configured prefix on the looked-up column truncates the key population
	require.True(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"v:4"}`, "v"))
	// mo_indexes keeps the declared spelling while the params key uses the
	// compared spelling: matching must be case-insensitive either way
	require.True(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"v:4"}`, "V"))
	require.True(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"V:4"}`, "v"))
	// a prefix configured on some other column leaves this part alone
	require.False(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"v:4"}`, "id"))
	require.False(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"a:1,b:2"}`, "v"))
	// the lossless v2 encoding for delimiter-bearing names is decoded too
	require.True(t, indexPartPrefixTruncatesColumn(
		`{"prefix_lengths_v2":"{\"head:line\":4}"}`, "Head:Line"))
	// a composite index with a prefix on its second part protects that part's
	// own column when it is looked up
	require.False(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"tail:2"}`, "head"))
	// undecodable metadata must not publish a guess
	require.True(t, indexPartPrefixTruncatesColumn("{not json", "v"))
	require.True(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"v:bad"}`, "v"))
	require.True(t, indexPartPrefixTruncatesColumn(`{"prefix_lengths":"v:0"}`, "v"))
}

func TestNdvEstimateFromStatsInfoPublishedValues(t *testing.T) {
	// an analyzed empty table publishes an explicit zero: known, not missing
	empty := &statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"id": 0, "v": 0},
		TableCnt: 0,
	}
	require.NotNil(t, ndvEstimateFromStatsInfo(empty, "id"))
	require.Zero(t, *ndvEstimateFromStatsInfo(empty, "id"))

	// a populated population reports its published estimate, rounded to the
	// nearest integer (half away from zero) and capped by the published table
	// row count
	populated := &statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"id": 8, "v": 8, "low": 3.49, "half": 5.5},
		TableCnt: 8,
	}
	require.EqualValues(t, 8, *ndvEstimateFromStatsInfo(populated, "id"))
	require.EqualValues(t, 3, *ndvEstimateFromStatsInfo(populated, "low"))
	require.EqualValues(t, 6, *ndvEstimateFromStatsInfo(populated, "half"))
	// an estimate beyond the row count collapses onto the row count
	require.EqualValues(t, 8, *ndvEstimateFromStatsInfo(populated, "v"))

	// a transient row count is not misread as a finite cap
	transient := &statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"id": 12},
		TableCnt: math.Inf(1),
	}
	require.EqualValues(t, 12, *ndvEstimateFromStatsInfo(transient, "id"))
}

// TestNullCntFromStatsInfoPublishesNullPopulation pins the NULL-group review
// finding (#30910): MySQL index statistics count the NULL rows as exactly one
// additional key group, so per-column cardinality needs the published NULL
// population beside the distinct-value estimate. The accessor keeps the same
// loss boundary as the NDV estimate: a missing statistics object or an
// unpublished column means unknown and renders NULL, while the explicit zero
// an analyzed no-NULL table publishes stays a meaningful known zero.
func TestNullCntFromStatsInfoPublishesNullPopulation(t *testing.T) {
	require.Nil(t, nullCntFromStatsInfo(nil, "v"))
	require.Nil(t, nullCntFromStatsInfo(&statsinfopb.StatsInfo{}, "v"))
	require.Nil(t, nullCntFromStatsInfo(
		&statsinfopb.StatsInfo{NullCntMap: map[string]uint64{"id": 3}}, "v"))

	// an analyzed table with no NULL rows publishes an explicit zero
	analyzed := &statsinfopb.StatsInfo{NullCntMap: map[string]uint64{"id": 0, "v": 3}}
	known := nullCntFromStatsInfo(analyzed, "id")
	require.NotNil(t, known)
	require.Zero(t, *known)
	// an all-NULL column publishes its whole population so the missing-value
	// group is counted once by the caller instead of vanishing
	require.EqualValues(t, 3, *nullCntFromStatsInfo(analyzed, "v"))

	// the declared spelling resolves to the canonical lowercased key, the
	// same fallback the distinct-value accessor uses
	require.EqualValues(t, 3, *nullCntFromStatsInfo(analyzed, "V"))
	upper := &statsinfopb.StatsInfo{NullCntMap: map[string]uint64{"UPPER": 7}}
	require.EqualValues(t, 7, *nullCntFromStatsInfo(upper, "UPPER"))
	require.Nil(t, nullCntFromStatsInfo(upper, "missing"))

	// a population beyond int64 collapses onto the representable maximum
	require.EqualValues(t, math.MaxInt64, *nullCntFromStatsInfo(
		&statsinfopb.StatsInfo{NullCntMap: map[string]uint64{"v": math.MaxUint64}}, "v"))
}

// indexSizeCandidateTableNames must keep the SHOW TABLE STATUS Index_length
// scope: only hidden unique/secondary objects, with the legacy empty
// IndexTableName tolerated instead of failing the whole call.
func TestIndexSizeCandidateTableNames(t *testing.T) {
	require.Empty(t, indexSizeCandidateTableNames(nil))
	require.Empty(t, indexSizeCandidateTableNames(&plan.TableDef{}))

	def := &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexTableName: catalog.UniqueIndexTableNamePrefix + "01"},
		{IndexTableName: catalog.SecondaryIndexTableNamePrefix + "02"},
		{IndexTableName: catalog.FullTextIndexTableNamePrefix + "03"},
		{IndexTableName: catalog.SecondaryIndexTableNamePrefix},
		{IndexTableName: ""},
		nil,
	}}
	require.Equal(t, []string{
		catalog.UniqueIndexTableNamePrefix + "01",
		catalog.SecondaryIndexTableNamePrefix + "02",
		catalog.SecondaryIndexTableNamePrefix,
	}, indexSizeCandidateTableNames(def))
}

// The new builtins ride the existing mo_table_stats session variable
// contract: both must be registered as strict volatile realtime functions so
// a changing statistics snapshot never gets folded into a constant plan.
func TestMoTableStatsPublicationFunctionsRegistered(t *testing.T) {
	found := map[int]bool{}
	for _, fun := range allSupportedFunctions {
		if fun.functionId == MO_TABLE_COL_NDV || fun.functionId == MO_TABLE_COL_NULLCNT ||
			fun.functionId == MO_TABLE_INDEX_SIZE || fun.functionId == MO_TABLE_DATA_SIZE {
			found[fun.functionId] = true
			for _, overload := range fun.Overloads {
				require.Equal(t, plan.Function_STRICT, fun.class)
				require.True(t, overload.volatile)
				require.True(t, overload.realTimeRelated)
				require.Equal(t, types.T_int64.ToType(), overload.retType(nil))
			}
		}
	}
	require.True(t, found[MO_TABLE_COL_NDV])
	require.True(t, found[MO_TABLE_COL_NULLCNT])
	require.True(t, found[MO_TABLE_INDEX_SIZE])
	require.True(t, found[MO_TABLE_DATA_SIZE])

	// mo_table_col_ndv exposes three arities (#30910): the plain estimator,
	// the metadata-guarded form the STATISTICS view projects, and the
	// publication-bound form whose last input carries a subscription branch's
	// in-band publisher account config. mo_table_col_nullcnt adds the same
	// publication-bound arity beside its plain form. Every overload stays
	// volatile and realtime so a changing statistics snapshot is never folded
	// into a constant plan.
	for _, fun := range allSupportedFunctions {
		switch fun.functionId {
		case MO_TABLE_COL_NDV:
			require.Len(t, fun.Overloads, 3)
			require.Equal(t, []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				fun.Overloads[0].args)
			require.Equal(t, []types.T{types.T_varchar, types.T_varchar, types.T_varchar, types.T_varchar},
				fun.Overloads[1].args)
			require.Equal(t, []types.T{
				types.T_varchar, types.T_varchar, types.T_varchar, types.T_varchar, types.T_varchar,
			}, fun.Overloads[2].args)
		case MO_TABLE_COL_NULLCNT:
			require.Len(t, fun.Overloads, 2)
			require.Equal(t, []types.T{types.T_varchar, types.T_varchar, types.T_varchar},
				fun.Overloads[0].args)
			require.Equal(t, []types.T{
				types.T_varchar, types.T_varchar, types.T_varchar, types.T_varchar,
			}, fun.Overloads[1].args)
		}
	}
}

// TestMoTableDataSizeLegacyModeDecomposition pins the review finding that
// DATA_LENGTH must stay sourced from base-table bytes in explicit legacy
// statistics mode too: the legacy mo_table_size implementation reports data
// plus index bytes (37 + 11 = 48), so mo_table_data_size must publish the
// base bytes alone while mo_table_index_size keeps the hidden index bytes,
// and the decomposition must still add up to the legacy total.
func TestMoTableDataSizeLegacyModeDecomposition(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	idxRel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn
	proc.SetResolveVariableFunc(func(variable string, _, _ bool) (any, error) {
		if variable == MoTableRowSizeUseOldImplVarName {
			return "yes", nil
		}
		return "no", nil
	})

	hidden := catalog.SecondaryIndexTableNamePrefix + "0123"
	eng.EXPECT().Database(gomock.Any(), "app", txn).Return(db, nil).AnyTimes()
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), hidden, nil).Return(idxRel, nil).AnyTimes()
	rel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(37), nil).AnyTimes()
	rel.EXPECT().GetTableName().Return("table").AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Indexes: []*plan.IndexDef{{IndexTableName: hidden}},
	}).AnyTimes()
	idxRel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(11), nil).AnyTimes()
	idxRel.EXPECT().GetTableName().Return(hidden).AnyTimes()

	callEntry := func(entry executeLogicOfOverload) int64 {
		dbv, tv, result := moTableStatsCallVector(t, proc, "app", "table")
		defer dbv.Free(proc.Mp())
		defer tv.Free(proc.Mp())
		defer result.Free()
		require.NoError(t, entry([]*vector.Vector{dbv, tv}, result, proc, 1, nil))
		return vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0]
	}

	dataBytes := callEntry(MoTableDataSize)
	indexBytes := callEntry(MoTableIndexSize)
	totalBytes := callEntry(MoTableSize)

	require.EqualValues(t, 37, dataBytes, "legacy data bytes must exclude the hidden index objects")
	require.EqualValues(t, 11, indexBytes)
	require.EqualValues(t, 48, totalBytes, "legacy mo_table_size keeps its data+index total contract")
	require.EqualValues(t, totalBytes, dataBytes+indexBytes,
		"data/index decomposition must preserve the legacy total")
	require.NotEqual(t, totalBytes, dataBytes, "a positive index size must not fold into data bytes")
}

// TestMoTableDataSizeEngineFallbackDecomposition pins the same contract on
// the engine-fallback path: when the registered statistics executor declines
// the batch (handled=false), every mo_table_* family member falls back to its
// per-relation implementation and the data/index decomposition still adds up
// without double counting.
func TestMoTableDataSizeEngineFallbackDecomposition(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	idxRel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return "no", nil
	})

	hidden := catalog.SecondaryIndexTableNamePrefix + "0123"
	eng.EXPECT().Database(gomock.Any(), "app", txn).Return(db, nil).AnyTimes()
	db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), hidden, nil).Return(idxRel, nil).AnyTimes()
	rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(10)).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
	rel.EXPECT().GetTableName().Return("table").AnyTimes()
	rel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(37), nil).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Indexes: []*plan.IndexDef{{IndexTableName: hidden}},
	}).AnyTimes()
	idxRel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(11), nil).AnyTimes()
	idxRel.EXPECT().GetTableName().Return(hidden).AnyTimes()

	callback := GetMoTableSizeRowsFuncType(func(
		context.Context, engine.Engine, MoTableStatsResolver, bool, bool,
	) ([]uint64, error, bool) {
		return nil, nil, false
	})
	previous := GetMoTableSizeFunc.Swap(&callback)
	defer GetMoTableSizeFunc.Store(previous)

	callEntry := func(entry executeLogicOfOverload) int64 {
		dbv, tv, result := moTableStatsCallVector(t, proc, "app", "table")
		defer dbv.Free(proc.Mp())
		defer tv.Free(proc.Mp())
		defer result.Free()
		require.NoError(t, entry([]*vector.Vector{dbv, tv}, result, proc, 1, nil))
		return vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0]
	}

	dataBytes := callEntry(MoTableDataSize)
	indexBytes := callEntry(MoTableIndexSize)
	totalBytes := callEntry(MoTableSize)

	require.EqualValues(t, 37, dataBytes, "engine fallback must keep data bytes free of index bytes")
	require.EqualValues(t, 11, indexBytes)
	require.EqualValues(t, 48, totalBytes)
	require.EqualValues(t, totalBytes, dataBytes+indexBytes)
	require.NotEqual(t, totalBytes, dataBytes)
}

// TestSubscriptionResolverQuotesIdentifiers pins the review finding that the
// shared subscription resolver must quote the publisher's database and table
// literals: a subscribed table named odd'name from a publishing database
// named orig'in flows through information_schema.TABLES statistics reads and
// must resolve, not break the catalog query. Resolution must also succeed on
// the follow-up call in the same process (continuation).
func TestSubscriptionResolverQuotesIdentifiers(t *testing.T) {
	t.Run("legacy old mode", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		subdb := mock_frontend.NewMockDatabase(ctrl)
		origin := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
		proc.Base.TxnOperator = txn
		proc.SetResolveVariableFunc(func(variable string, _, _ bool) (any, error) {
			if variable == MoTableRowSizeUseOldImplVarName {
				return "yes", nil
			}
			return "no", nil
		})
		helper := &quotingAwareSubscriptionSQL{}
		proc.Base.SessionInfo.SqlHelper = helper

		eng.EXPECT().Database(gomock.Any(), "sub", txn).Return(subdb, nil).AnyTimes()
		subdb.EXPECT().IsSubscription(gomock.Any()).Return(true).AnyTimes()
		// the publishing account's database itself carries a quote
		eng.EXPECT().Database(gomock.Any(), "orig'in", txn).Return(origin, nil).AnyTimes()
		origin.EXPECT().Relation(gomock.Any(), "odd'name", nil).Return(rel, nil).AnyTimes()
		rel.EXPECT().Size(gomock.Any(), AllColumns).Return(uint64(37), nil).AnyTimes()
		// the definition-access guard added on main reads the relation name
		rel.EXPECT().GetTableName().Return("odd'name").AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()

		callEntry := func(entry executeLogicOfOverload) int64 {
			dbv, tv, result := moTableStatsCallVector(t, proc, "sub", "odd'name")
			defer dbv.Free(proc.Mp())
			defer tv.Free(proc.Mp())
			defer result.Free()
			require.NoError(t, entry([]*vector.Vector{dbv, tv}, result, proc, 1, nil))
			return vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0]
		}

		require.EqualValues(t, 37, callEntry(MoTableSize))
		require.EqualValues(t, 37, callEntry(MoTableDataSize), "the second read must keep resolving (continuation)")
		require.Len(t, helper.queries, 2)
		for _, sql := range helper.queries {
			require.Contains(t, sql, "reldatabase = "+sqlquote.String("orig'in"))
			require.Contains(t, sql, "relname = "+sqlquote.String("odd'name"))
			require.NotContains(t, sql, "relname = 'odd'name'", "the raw quote must never reach the SQL")
		}
	})

	t.Run("new statistics mode", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		eng := mock_frontend.NewMockEngine(ctrl)
		subdb := mock_frontend.NewMockDatabase(ctrl)
		origin := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		txn := mock_frontend.NewMockTxnOperator(ctrl)
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
		proc.Base.TxnOperator = txn
		proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
			return "no", nil
		})
		helper := &quotingAwareSubscriptionSQL{}
		proc.Base.SessionInfo.SqlHelper = helper

		eng.EXPECT().Database(gomock.Any(), "sub", txn).Return(subdb, nil).AnyTimes()
		subdb.EXPECT().IsSubscription(gomock.Any()).Return(true).AnyTimes()
		// checkSubscribedIndexRelation resolves the published relation in the
		// publisher's account, validates the recorded ids against it and reads
		// its index-relation definition before the resolver reports the ids.
		eng.EXPECT().Database(gomock.Any(), "orig'in", txn).Return(origin, nil).AnyTimes()
		origin.EXPECT().Relation(gomock.Any(), "odd'name", nil).Return(rel, nil).AnyTimes()
		rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(10)).AnyTimes()
		rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(20)).AnyTimes()
		rel.EXPECT().GetTableName().Return("odd'name").AnyTimes()
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()

		callback := GetMoTableSizeRowsFuncType(func(
			_ context.Context, _ engine.Engine, resolve MoTableStatsResolver, _, _ bool,
		) ([]uint64, error, bool) {
			accs, dbs, tbls, err := resolve()
			require.NoError(t, err)
			require.Equal(t, []uint64{2}, accs)
			require.Equal(t, []uint64{10}, dbs)
			require.Equal(t, []uint64{20}, tbls)
			return []uint64{37}, nil, true
		})
		previous := GetMoTableSizeFunc.Swap(&callback)
		defer GetMoTableSizeFunc.Store(previous)

		callEntry := func(entry executeLogicOfOverload) int64 {
			dbv, tv, result := moTableStatsCallVector(t, proc, "sub", "odd'name")
			defer dbv.Free(proc.Mp())
			defer tv.Free(proc.Mp())
			defer result.Free()
			require.NoError(t, entry([]*vector.Vector{dbv, tv}, result, proc, 1, nil))
			return vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0]
		}

		require.EqualValues(t, 37, callEntry(MoTableDataSize))
		require.EqualValues(t, 37, callEntry(MoTableSize))
		require.Len(t, helper.queries, 2)
		for _, sql := range helper.queries {
			require.Contains(t, sql, "reldatabase = "+sqlquote.String("orig'in"))
			require.Contains(t, sql, "relname = "+sqlquote.String("odd'name"))
		}
	})
}

// quotingAwareSubscriptionSQL records every catalog SQL the shared
// subscription resolver interpolates so a quoted identifier cannot silently
// regress into an unescaped literal.
type quotingAwareSubscriptionSQL struct {
	meta    int
	queries []string
}

func (s *quotingAwareSubscriptionSQL) GetCompilerContext() any { return nil }
func (s *quotingAwareSubscriptionSQL) ExecSql(string) ([][]interface{}, error) {
	panic("unexpected contextless SQL")
}
func (s *quotingAwareSubscriptionSQL) GetSubscriptionMeta(string) (*plan.SubscriptionMeta, error) {
	s.meta++
	return &plan.SubscriptionMeta{
		AccountId: 2, AccountName: "publisher",
		DbName: "orig'in", Tables: "*",
	}, nil
}
func (s *quotingAwareSubscriptionSQL) ExecSqlWithCtx(_ context.Context, sql string) ([][]interface{}, error) {
	s.queries = append(s.queries, sql)
	return [][]interface{}{{uint64(10), uint64(20)}}, nil
}

// TestPublicationAccountIDFromConfig pins the in-band publisher-account
// config grammar (#30910): only the exact pubacct=<uint32> spelling binds a
// call to a publication branch. Zero is the sys account id and binds like any
// other id, because publications created by the sys session record
// mo_subs.pub_account_id = 0. Anything else - a missing prefix, an empty or
// non-numeric suffix or an out-of-range id - leaves the call unbound with
// its direct resolution semantics.
func TestPublicationAccountIDFromConfig(t *testing.T) {
	id, bound := publicationAccountIDFromConfig("pubacct=17")
	require.True(t, bound)
	require.EqualValues(t, 17, id)

	id, bound = publicationAccountIDFromConfig("pubacct=4294967295")
	require.True(t, bound)
	require.EqualValues(t, 4294967295, id)

	// A sys-published subscription (every `from sys publication` shape) bakes
	// the sys account id and must bind, never fall back to direct resolution.
	id, bound = publicationAccountIDFromConfig("pubacct=0")
	require.True(t, bound)
	require.EqualValues(t, 0, id)

	for _, config := range []string{
		"",
		"17",
		"pubacct",
		"pubacct=",
		"pubacct=-1",
		"pubacct=x",
		"pubacct=17x",
		"PUBACCT=17",
		"pubacct=4294967296",
	} {
		id, bound = publicationAccountIDFromConfig(config)
		require.False(t, bound, "config %q must not bind a call", config)
		require.Zero(t, id)
	}
}

// TestPublicationAccountIDFromVecArg pins the row-level config read: a NULL
// config argument leaves the row unbound instead of binding account zero.
func TestPublicationAccountIDFromVecArg(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	configured, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte("pubacct=9"), 1, proc.Mp())
	require.NoError(t, err)
	defer configured.Free(proc.Mp())
	id, bound := publicationAccountIDFromVecArg(
		vector.GenerateFunctionStrParameter(configured), 0)
	require.True(t, bound)
	require.EqualValues(t, 9, id)

	absent := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer absent.Free(proc.Mp())
	id, bound = publicationAccountIDFromVecArg(
		vector.GenerateFunctionStrParameter(absent), 0)
	require.False(t, bound, "a NULL config must leave the row unbound")
	require.Zero(t, id)

	require.False(t, func() bool {
		id, bound = publicationAccountIDFromVecArg(nil, 0)
		return bound
	}())
	require.Zero(t, id)
}

// TestMoTableColNdvPublicationBoundResolvesThroughBakedAccount pins the
// parallel-scope regression of the subscription STATISTICS read (#30910): a
// publisher branch's estimate must resolve entirely through the publisher
// account baked into its argument, never through a per-row internal
// subscription lookup on the shared session executor. The mock engine answers
// only requests that carry the baked publisher account, and the process has no
// subscription helper at all, so any internal metadata resolution fails this
// test loudly instead of silently interleaving internal result sets across
// parallel union arms.
func TestMoTableColNdvPublicationBoundResolvesThroughBakedAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	pubdb := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	// the reading session belongs to the subscriber account
	proc.Ctx = context.WithValue(
		defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "pub_db", txn).DoAndReturn(
		func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
			id, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 17, id,
				"the estimate must resolve through the baked publisher account")
			return pubdb, nil
		}).AnyTimes()
	pubdb.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).AnyTimes()
	rel.EXPECT().Stats(gomock.Any(), false).Return(&statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"v": 5},
		TableCnt: 5,
	}, nil).AnyTimes()

	ivecs, result := estimatorCallVector(t, proc, "pub_db", "t", "v", "", "pubacct=17")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNdv(ivecs, result, proc, 1, nil))

	out := result.GetResultVector()
	require.False(t, out.IsNull(0))
	require.EqualValues(t, 5, vector.MustFixedColWithTypeCheck[int64](out)[0])
}

// TestMoTableColNdvPublicationSysPublisherResolvesThroughBakedAccount pins
// the sys-publisher shape of the binding (#30910): every `from sys
// publication` subscription records mo_subs.pub_account_id = 0, so the baked
// argument is "pubacct=0" and must bind and resolve through the sys account
// exactly like a foreign publisher id; it must never fall back to the direct
// resolution that names the physical published database under the subscriber
// account.
func TestMoTableColNdvPublicationSysPublisherResolvesThroughBakedAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	pubdb := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	// the reading session belongs to the subscriber account
	proc.Ctx = context.WithValue(
		defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "pub_db", txn).DoAndReturn(
		func(ctx context.Context, _ string, _ client.TxnOperator) (engine.Database, error) {
			id, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, id,
				"a sys publication must resolve through the baked sys account")
			return pubdb, nil
		}).AnyTimes()
	pubdb.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).AnyTimes()
	rel.EXPECT().Stats(gomock.Any(), false).Return(&statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"v": 5},
		TableCnt: 5,
	}, nil).AnyTimes()

	ivecs, result := estimatorCallVector(t, proc, "pub_db", "t", "v", "", "pubacct=0")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNdv(ivecs, result, proc, 1, nil))

	out := result.GetResultVector()
	require.False(t, out.IsNull(0))
	require.EqualValues(t, 5, vector.MustFixedColWithTypeCheck[int64](out)[0])
}

// TestMoTableColNdvPublicationBoundRendersNullOnUnresolvable pins the
// snapshot-facing boundary (#30910): a publication-bound estimate for a name
// that no longer resolves at the reading transaction - for example a database
// dropped after the snapshot was taken - publishes the unavailable estimate
// as NULL instead of failing the whole metadata read.
func TestMoTableColNdvPublicationBoundRendersNullOnUnresolvable(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(
		defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "dropped_db", txn).Return(
		nil, moerr.NewInternalErrorNoCtx("db not exist: dropped_db")).AnyTimes()

	ivecs, result := estimatorCallVector(
		t, proc, "dropped_db", "t", "v", "", "pubacct=17")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNdv(ivecs, result, proc, 1, nil))

	out := result.GetResultVector()
	require.True(t, out.IsNull(0),
		"an unresolvable publication-bound name must render NULL, never an error")
}

// TestMoTableColNdvPublicationBoundPreservesPrefixGuard pins that binding a
// branch to its publisher account does not bypass the truncated-prefix guard:
// a key part configured as col(n) has no per-prefix estimate, so its
// publication-bound cardinality renders NULL even when the column's own
// estimate resolves.
func TestMoTableColNdvPublicationBoundPreservesPrefixGuard(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	pubdb := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(
		defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "pub_db", txn).Return(pubdb, nil).AnyTimes()
	pubdb.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).AnyTimes()
	rel.EXPECT().Stats(gomock.Any(), false).Return(&statsinfopb.StatsInfo{
		NdvMap:   map[string]float64{"v": 5},
		TableCnt: 5,
	}, nil).AnyTimes()

	ivecs, result := estimatorCallVector(
		t, proc, "pub_db", "t", "v", `{"prefix_lengths":"v:4"}`, "pubacct=17")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNdv(ivecs, result, proc, 1, nil))

	out := result.GetResultVector()
	require.True(t, out.IsNull(0),
		"a truncated-prefix key part has no published estimate")
}

// TestMoTableColNullCntPublicationBoundCoversTheNullGroup pins the same
// publication-bound resolution for the NULL-population projection: the baked
// publisher account answers the bound call, and an unresolvable name renders
// the unavailable NULL population instead of failing the metadata read.
func TestMoTableColNullCntPublicationBoundCoversTheNullGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	pubdb := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = context.WithValue(
		defines.AttachAccountId(context.Background(), 1), defines.EngineKey{}, eng)
	proc.Base.TxnOperator = txn

	eng.EXPECT().Database(gomock.Any(), "pub_db", txn).Return(pubdb, nil).AnyTimes()
	pubdb.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil).AnyTimes()
	rel.EXPECT().Stats(gomock.Any(), false).Return(&statsinfopb.StatsInfo{
		NullCntMap: map[string]uint64{"v": 3},
	}, nil).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), "gone_db", txn).Return(
		nil, moerr.NewInternalErrorNoCtx("db not exist: gone_db")).AnyTimes()

	ivecs, result := estimatorCallVector(t, proc, "pub_db", "t", "v", "pubacct=17")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNullCnt(ivecs, result, proc, 1, nil))
	out := result.GetResultVector()
	require.False(t, out.IsNull(0))
	require.EqualValues(t, 3, vector.MustFixedColWithTypeCheck[int64](out)[0])

	ivecs, result = estimatorCallVector(t, proc, "gone_db", "t", "v", "pubacct=17")
	for _, vec := range ivecs {
		defer vec.Free(proc.Mp())
	}
	defer result.Free()
	require.NoError(t, MoTableColNullCnt(ivecs, result, proc, 1, nil))
	out = result.GetResultVector()
	require.True(t, out.IsNull(0),
		"an unresolvable publication-bound NULL population renders NULL")
}

// estimatorCallVector builds one varchar const-argument row in the shared
// layout of the statistics estimators: database, table, column, then
// optionally the index algo_params and publication-config arguments. The
// caller owns Free-ing the vectors, the wrapper and the process.
func estimatorCallVector(
	t *testing.T, proc *process.Process, values ...string,
) ([]*vector.Vector, vector.FunctionResultWrapper) {
	vecs := make([]*vector.Vector, 0, len(values))
	for _, value := range values {
		vec, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(value), 1, proc.Mp())
		require.NoError(t, err)
		vecs = append(vecs, vec)
	}
	wrapper := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, wrapper.PreExtendAndReset(1))
	return vecs, wrapper
}

// moTableStatsCallVector builds one (db, table) input row for a scalar
// mo_table_* call and returns the pair of argument vectors with a prepared
// int64 result wrapper. The caller owns Free-ing the vectors, the wrapper and
// the process.
func moTableStatsCallVector(t *testing.T, proc *process.Process, db, table string) (
	*vector.Vector, *vector.Vector, vector.FunctionResultWrapper,
) {
	dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(db), 1, proc.Mp())
	require.NoError(t, err)
	tv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(table), 1, proc.Mp())
	require.NoError(t, err)
	wrapper := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, wrapper.PreExtendAndReset(1))
	return dbv, tv, wrapper
}
