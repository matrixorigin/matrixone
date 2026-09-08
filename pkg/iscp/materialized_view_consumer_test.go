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

package iscp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

type materializedViewTestRelation struct {
	engine.Relation
	rows [][]any
	def  *planpb.TableDef
}

func (r *materializedViewTestRelation) ReadRowsByRowID(
	context.Context, []types.Rowid, types.TS, []string, *mpool.MPool, *engine.RowIDReadBudget,
) ([][]any, error) {
	return r.rows, nil
}

type materializedViewBoundaryRetriever struct {
	MockRetriever
	from types.TS
	to   types.TS
}

func (r *materializedViewBoundaryRetriever) GetFromTS() types.TS { return r.from }
func (r *materializedViewBoundaryRetriever) GetToTS() types.TS   { return r.to }

type materializedViewToBoundaryRetriever struct {
	MockRetriever
	to types.TS
}

func (r *materializedViewToBoundaryRetriever) GetToTS() types.TS { return r.to }

type materializedViewErrorRetriever struct {
	MockRetriever
	err error
}

func (r *materializedViewErrorRetriever) Next() *ISCPData {
	data := &ISCPData{err: r.err}
	data.Set(0)
	return data
}

func encodeMaterializedViewIncrementalDescription(t *testing.T, desc incrementalDescription) string {
	t.Helper()
	b, err := json.Marshal(desc)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(b)
}

func TestNewMaterializedViewConsumerValidatesSpec(t *testing.T) {
	_, err := NewMaterializedViewConsumer("", nil, nil, JobID{}, nil)
	require.Error(t, err)

	info := &ConsumerInfo{DBName: "db", TableName: "mv", RefreshSQL: "select count(*) from events"}
	newMVTestCatalog(t, info)
	consumer, err := NewMaterializedViewConsumer("", nil, nil, JobID{}, info)
	require.NoError(t, err)
	require.IsType(t, &MaterializedViewConsumer{}, consumer)
}

func TestMaterializedViewConsumerNeedsTailPayloadOnlyForIncrementalRefresh(t *testing.T) {
	consumer := &MaterializedViewConsumer{info: &ConsumerInfo{IncrementalSpec: "incremental"}}
	require.True(t, consumer.NeedsChangePayload(ISCPDataType_Tail))
	require.False(t, consumer.NeedsChangePayload(ISCPDataType_Snapshot))
	consumer.info.IncrementalSpec = ""
	require.False(t, consumer.NeedsChangePayload(ISCPDataType_Tail))
}

func TestRefreshMaterializedViewOnDemandUsesCallerTransaction(t *testing.T) {
	info := &ConsumerInfo{DBName: "db", TableName: "mv", Columns: []string{"service", "requests"}, RefreshSQL: "select service, count(*) requests from events group by service"}
	c := newMVTestCatalog(t, info)
	err := RefreshMaterializedView(defines.AttachAccountId(t.Context(), 0), c, "cn", c.txn, info, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"delete from `db`.`mv` where `__mo_fake_pk_col` is not null",
		"insert into `db`.`mv` (`service`,`requests`,`__mo_fake_pk_col`) select `service`,`requests`, row_number() over () from (select `service`, count(*) as `requests` from `db`.`events` group by `service`) as `__mo_mv_refresh` (`service`,`requests`)",
	}, c.sqls)
}

func TestMaterializedViewRefreshAtIterationBoundary(t *testing.T) {
	ts := types.BuildTS(100, 7)
	sources := []TableInfo{{DBName: "db", TableName: "src"}}
	query, err := materializedViewRefreshAtSources("select src, count(*) from src group by src", sources, ts)
	require.NoError(t, err)
	require.Equal(t, "select `src`, count(*) from `db`.`src`{MO_TS = '100-7'} group by `src`", query)

	_, err = materializedViewRefreshAtSources("select 1", sources, ts)
	require.Error(t, err)
}

func TestMaterializedViewRefreshAtMultipleSources(t *testing.T) {
	ts := types.BuildTS(100, 7)
	query, err := materializedViewRefreshAtSources(
		"select * from db1.a as x join db2.b as y on x.id = y.id",
		[]TableInfo{{DBName: "db1", TableName: "a"}, {DBName: "db2", TableName: "b"}}, ts)
	require.NoError(t, err)
	require.Equal(t,
		"select * from `db1`.`a`{MO_TS = '100-7'} as `x` inner join `db2`.`b`{MO_TS = '100-7'} as `y` on `x`.`id` = `y`.`id`",
		query)
}

func TestMaterializedViewRefreshAtCommaJoinSources(t *testing.T) {
	ts := types.BuildTS(100, 7)
	query, err := materializedViewRefreshAtSources(
		"select x.id, y.id from db1.a as x, db2.b as y where x.id = y.id",
		[]TableInfo{
			{DBName: "db1", TableName: "a"},
			{DBName: "db2", TableName: "b"},
		}, ts)
	require.NoError(t, err)
	require.Equal(t,
		"select `x`.`id`, `y`.`id` from `db1`.`a`{MO_TS = '100-7'} as `x` cross join `db2`.`b`{MO_TS = '100-7'} as `y` where `x`.`id` = `y`.`id`",
		query)
}

func TestMaterializedViewRefreshAtSourcesDoesNotRewriteColumnReferences(t *testing.T) {
	ts := types.BuildTS(100, 7)
	query, err := materializedViewRefreshAtSources(
		"select l_returnflag, l_linestatus, sum(l_quantity) from lineitem group by l_returnflag, l_linestatus",
		[]TableInfo{{DBName: "tpch", TableName: "lineitem"}}, ts)
	require.NoError(t, err)
	require.Equal(t,
		"select `l_returnflag`, `l_linestatus`, sum(`l_quantity`) from `tpch`.`lineitem`{MO_TS = '100-7'} group by `l_returnflag`, `l_linestatus`",
		query)
}

func TestMaterializedViewRefreshAtSourcesPreservesStringLiterals(t *testing.T) {
	ts := types.BuildTS(100, 7)
	query, err := materializedViewRefreshAtSources(
		"select date_trunc('minute', event_ts), count(*) from events where status >= 500 group by date_trunc('minute', event_ts)",
		[]TableInfo{{DBName: "observability", TableName: "events"}}, ts)
	require.NoError(t, err)
	require.Equal(t,
		"select date_trunc('minute', `event_ts`), count(*) from `observability`.`events`{MO_TS = '100-7'} where `status` >= 500 group by date_trunc('minute', `event_ts`)",
		query)
}

func TestMaterializedViewRefreshSourceValidation(t *testing.T) {
	ts := types.BuildTS(100, 7)
	tests := []struct {
		name    string
		query   string
		sources []TableInfo
		wantErr string
	}{
		{name: "no sources", query: "select * from events", wantErr: "no source tables"},
		{name: "invalid sql", query: "select from", sources: []TableInfo{{DBName: "db", TableName: "events"}}, wantErr: "parse materialized view refresh query"},
		{name: "not select", query: "delete from events", sources: []TableInfo{{DBName: "db", TableName: "events"}}, wantErr: "expected select"},
		{name: "no from", query: "select 1", sources: []TableInfo{{DBName: "db", TableName: "events"}}, wantErr: "not found in refresh metadata"},
		{name: "incomplete metadata", query: "select * from events", sources: []TableInfo{{TableName: "events"}}, wantErr: "incomplete source table"},
		{name: "query table absent from metadata", query: "select * from other", sources: []TableInfo{{DBName: "db", TableName: "events"}}, wantErr: "not found in refresh metadata"},
		{name: "ambiguous unqualified source", query: "select * from events", sources: []TableInfo{{DBName: "a", TableName: "events"}, {DBName: "b", TableName: "events"}}, wantErr: "ambiguous without a database qualifier"},
		{name: "metadata table absent from query", query: "select * from db.events", sources: []TableInfo{{DBName: "db", TableName: "events"}, {DBName: "db", TableName: "metadata"}}, wantErr: "source \"metadata\" not found"},
		{name: "derived source", query: "select * from (select * from events) x", sources: []TableInfo{{DBName: "db", TableName: "events"}}, wantErr: "direct base table"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := materializedViewRefreshAtSources(tc.query, tc.sources, ts)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestRefreshMaterializedViewFailureBoundaries(t *testing.T) {
	require.Error(t, RefreshMaterializedView(t.Context(), nil, "cn", nil, nil, nil))
	for _, prefix := range []string{"delete from", "insert into"} {
		t.Run(prefix, func(t *testing.T) {
			info := &ConsumerInfo{DBName: "db", TableName: "mv", RefreshSQL: "select service, count(*) requests from events group by service"}
			c := newMVTestCatalog(t, info)
			failure := errors.New(prefix + " failed")
			c.failSQL = func(sql string) error {
				if strings.HasPrefix(sql, prefix) {
					return failure
				}
				return nil
			}
			err := RefreshMaterializedView(defines.AttachAccountId(t.Context(), 0), c, "cn", c.txn, info, nil)
			require.ErrorIs(t, err, failure)
			require.True(t, strings.HasPrefix(c.sqls[len(c.sqls)-1], prefix), "no later statement may run after failure")
		})
	}
	t.Run("invalid source identity before any DML", func(t *testing.T) {
		info := &ConsumerInfo{DBName: "db", TableName: "mv", RefreshSQL: "select * from events"}
		c := newMVTestCatalog(t, info)
		c.relations["db.events"].def.TblId++
		err := RefreshMaterializedView(defines.AttachAccountId(t.Context(), 0), c, "cn", c.txn, info, nil)
		require.ErrorContains(t, err, "changed")
		require.Empty(t, c.sqls)
	})
}

func TestRefreshIncrementalMaterializedViewAtBoundary(t *testing.T) {
	oldExec := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExec })

	desc := incrementalDescription{
		Version: 2, Strategy: "direct-delta", SourceAlias: "e", SourceColumns: []string{"service"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"},
	}
	boundary := types.BuildTS(100, 7)
	info := &ConsumerInfo{DBName: "db", TableName: "mv", Columns: []string{"service", "requests"}, RefreshSQL: "select e.service as service, count(*) as requests, count(*) as __row_count, serial_full(e.service) as __group_key from events as e group by e.service", IncrementalSpec: encodeMaterializedViewIncrementalDescription(t, desc)}
	c := newMVTestCatalog(t, info)
	err := RefreshMaterializedView(defines.AttachAccountId(t.Context(), 0), c, "cn", c.txn, info, &boundary)
	sqls := c.sqls
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "where `__group_key` is not null")
	require.Contains(t, sqls[1], "events`{MO_TS = '100-7'}")
	require.NotContains(t, sqls[1], "row_number()")
}

func TestMaterializedViewConsumerFullRefreshLifecycle(t *testing.T) {
	stubTxn := stubIndexConsumerTxnRunner()
	t.Cleanup(stubTxn.Reset)
	oldExec := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExec })

	fallbackSpec := encodeMaterializedViewIncrementalDescription(t, incrementalDescription{
		Version: 2, Strategy: "direct-delta", SourceAlias: "e", SourceColumns: []string{"service"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"},
	})
	for _, tc := range []struct {
		name              string
		dtype             int8
		incrementalSpec   string
		wantFullRefreshes int
	}{
		{name: "snapshot initialization", dtype: ISCPDataType_Snapshot, wantFullRefreshes: 1},
		{name: "tail fallback", dtype: ISCPDataType_Tail, incrementalSpec: fallbackSpec, wantFullRefreshes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {

			watermarks := 0
			retriever := &materializedViewToBoundaryRetriever{
				MockRetriever: MockRetriever{
					dtype: tc.dtype,
					updateWatermark: func(context.Context, string, client.TxnOperator) error {
						watermarks++
						return nil
					},
				},
				to: types.BuildTS(100, 7),
			}
			consumer := &MaterializedViewConsumer{cnUUID: "cn", info: &ConsumerInfo{
				DBName: "db", TableName: "mv", Columns: []string{"service", "requests"},
				RefreshSQL:      "select service, count(*) as requests from events group by service",
				SrcTables:       []TableInfo{{DBName: "db", TableName: "events"}},
				IncrementalSpec: tc.incrementalSpec,
			}}
			catalog := newMVTestCatalog(t, consumer.info)
			catalog.stubTransactions(t)
			consumer.cnEngine = catalog
			require.NoError(t, consumer.Consume(t.Context(), retriever))
			require.Equal(t, tc.wantFullRefreshes*2, len(catalog.sqls))
			require.Equal(t, 1, watermarks)
		})
	}
}

func TestMaterializedViewConsumerRefreshFailureBoundaries(t *testing.T) {
	stubTxn := stubIndexConsumerTxnRunner()
	t.Cleanup(stubTxn.Reset)

	drainErr := errors.New("change stream failed")
	consumer := &MaterializedViewConsumer{info: &ConsumerInfo{DBName: "db", TableName: "mv"}}
	err := consumer.Consume(t.Context(), &materializedViewErrorRetriever{
		MockRetriever: MockRetriever{dtype: ISCPDataType_Snapshot}, err: drainErr,
	})
	require.ErrorIs(t, err, drainErr)

	consumer.info.RefreshSQL = "select * from events"
	consumer.info.SrcTables = []TableInfo{{DBName: "db", TableName: "events"}}
	err = consumer.Consume(t.Context(), &MockRetriever{dtype: ISCPDataType_Snapshot})
	require.ErrorContains(t, err, "does not expose iteration boundary")
}

func TestMaterializedViewIncrementalSourceFailureBoundaries(t *testing.T) {
	stubTxn := stubIndexConsumerTxnRunner()
	t.Cleanup(stubTxn.Reset)
	desc := incrementalDescription{
		Version: 2, Strategy: "direct-delta", SourceAlias: "e", SourceColumns: []string{"service"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service"}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
		RowCountColumn: "__row_count", StateColumns: []string{"__row_count"},
	}
	sourceErr := errors.New("source unavailable")

	for _, tc := range []struct {
		name    string
		setup   func(*gomock.Controller) engine.Engine
		wantErr string
	}{
		{
			name: "database lookup",
			setup: func(ctrl *gomock.Controller) engine.Engine {
				eng := mock_frontend.NewMockEngine(ctrl)
				eng.EXPECT().Database(gomock.Any(), "srcdb", gomock.Any()).Return(nil, sourceErr)
				return eng
			},
			wantErr: sourceErr.Error(),
		},
		{
			name: "relation lookup",
			setup: func(ctrl *gomock.Controller) engine.Engine {
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				eng.EXPECT().Database(gomock.Any(), "srcdb", gomock.Any()).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "events", gomock.Any()).Return(nil, sourceErr)
				return eng
			},
			wantErr: sourceErr.Error(),
		},
		{
			name: "rowid lookup unsupported",
			setup: func(ctrl *gomock.Controller) engine.Engine {
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				eng.EXPECT().Database(gomock.Any(), "srcdb", gomock.Any()).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "events", gomock.Any()).Return(rel, nil)
				return eng
			},
			wantErr: "does not support rowid lookup",
		},
		{
			name: "missing table definition",
			setup: func(ctrl *gomock.Controller) engine.Engine {
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(nil)
				eng.EXPECT().Database(gomock.Any(), "srcdb", gomock.Any()).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "events", gomock.Any()).Return(&materializedViewTestRelation{Relation: rel}, nil)
				return eng
			},
			wantErr: "has no table definition",
		},
		{
			name: "unknown source column",
			setup: func(ctrl *gomock.Controller) engine.Engine {
				eng := mock_frontend.NewMockEngine(ctrl)
				db := mock_frontend.NewMockDatabase(ctrl)
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{Cols: []*planpb.ColDef{{Name: "other", Typ: planpb.Type{Id: int32(types.T_varchar)}}}})
				eng.EXPECT().Database(gomock.Any(), "srcdb", gomock.Any()).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "events", gomock.Any()).Return(&materializedViewTestRelation{Relation: rel}, nil)
				return eng
			},
			wantErr: "unknown column",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			consumer := &MaterializedViewConsumer{
				cnUUID: "cn", cnEngine: tc.setup(ctrl), jobID: JobID{DBName: "srcdb", TableName: "events"},
				info: &ConsumerInfo{
					DBName: "db", TableName: "mv", IncrementalSpec: encodeMaterializedViewIncrementalDescription(t, desc),
				},
			}
			_, _, err := consumer.materializedViewIncrementalRuntimes(t.Context(), nil, &desc)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestMaterializedViewDrainSkipsRowsForFullRefresh(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dtype int8
		spec  string
	}{
		{name: "snapshot", dtype: ISCPDataType_Snapshot, spec: "incremental"},
		{name: "tail without incremental spec", dtype: ISCPDataType_Tail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A nil Src would panic if materializedViewRowsFromBatch tried to
			// decode this row. Full-refresh paths must only drain and release it.
			rows := btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64})
			rows.Set(AtomicBatchRow{})
			r := &MockRetriever{
				dtype:       tc.dtype,
				insertBatch: &AtomicBatch{Rows: rows},
			}
			consumer := &MaterializedViewConsumer{info: &ConsumerInfo{
				DBName: "db", TableName: "mv", IncrementalSpec: tc.spec,
			}}
			require.NoError(t, consumer.drainChanges(r))
		})
	}
}

func TestMaterializedViewRowsFromBatchRejectsMalformedDeletes(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	var blockID types.Blockid
	rowID := types.NewRowid(&blockID, 1)

	for _, tc := range []struct {
		name    string
		attrs   []string
		vectors []*vector.Vector
		wantErr string
	}{
		{
			name: "missing rowid", attrs: []string{catalog.Row_ID},
			vectors: []*vector.Vector{testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{1})},
			wantErr: "does not retain rowid",
		},
		{
			name: "invalid commit timestamp", attrs: []string{catalog.Row_ID, "commit_ts"},
			vectors: []*vector.Vector{
				testutil.NewVector(1, types.T_Rowid.ToType(), mp, false, []types.Rowid{rowID}),
				testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{1}),
			},
			wantErr: "invalid commit timestamp",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bat := testutil.NewBatchWithVectors(tc.vectors, nil)
			bat.Attrs = tc.attrs
			t.Cleanup(func() { bat.Clean(mp) })
			atomic := NewAtomicBatch(mp)
			atomic.Batches = []*batch.Batch{bat}
			atomic.Rows.Set(AtomicBatchRow{Pk: []byte(tc.name), Src: bat})
			_, err := materializedViewRowsFromBatch(atomic, false)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestMaterializedViewFastRefreshDoesNotFallback(t *testing.T) {
	consumer := &MaterializedViewConsumer{info: &ConsumerInfo{
		DBName:          "db",
		TableName:       "mv",
		IncrementalSpec: "not-base64",
		RefreshMethod:   "fast",
	}}
	err := consumer.Consume(context.Background(), &MockRetriever{dtype: ISCPDataType_Tail})
	require.ErrorContains(t, err, "invalid materialized view incremental specification encoding")
}

func TestMaterializedViewConsumerAppliesInsertAndDeleteTail(t *testing.T) {
	const service = "materialized-view-consumer-tail-test"
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
	moruntime.SetupServiceBasedRuntime(service, rt)
	var sqls []string
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	}))
	stubTxn := stubIndexConsumerTxnRunner()
	t.Cleanup(stubTxn.Reset)

	tableDef := &planpb.TableDef{DbName: "srcdb", Name: "events", Cols: []*planpb.ColDef{{Name: "service", Typ: planpb.Type{Id: int32(types.T_varchar)}}, {Name: "value", Typ: planpb.Type{Id: int32(types.T_int64)}}}}

	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	var blockID types.Blockid
	rowID := types.NewRowid(&blockID, 1)
	insertBat := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(1, types.T_Rowid.ToType(), mp, false, []types.Rowid{rowID}),
		testutil.NewVector(1, types.T_varchar.ToType(), mp, false, []string{"api"}),
		testutil.NewVector(1, types.T_int64.ToType(), mp, false, []int64{10}),
	}, nil)
	insertBat.Attrs = []string{catalog.Row_ID, "service", "value"}
	deleteTS := types.BuildTS(20, 1)
	deleteBat := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(1, types.T_Rowid.ToType(), mp, false, []types.Rowid{rowID}),
		testutil.NewVector(1, types.T_TS.ToType(), mp, false, []types.TS{deleteTS}),
	}, nil)
	deleteBat.Attrs = []string{catalog.Row_ID, "commit_ts"}
	t.Cleanup(func() {
		insertBat.Clean(mp)
		deleteBat.Clean(mp)
	})
	atomicInsert := NewAtomicBatch(mp)
	atomicInsert.Batches = []*batch.Batch{insertBat}
	atomicInsert.Rows.Set(AtomicBatchRow{Pk: []byte("insert"), Src: insertBat})
	atomicDelete := NewAtomicBatch(mp)
	atomicDelete.Batches = []*batch.Batch{deleteBat}
	atomicDelete.Rows.Set(AtomicBatchRow{Pk: []byte("delete"), Src: deleteBat})

	desc := incrementalDescription{
		Version: 2, Strategy: "direct-delta", SourceAlias: "e", SourceColumns: []string{"service", "value"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"},
	}
	watermarkUpdates := 0
	retriever := &materializedViewBoundaryRetriever{
		MockRetriever: MockRetriever{
			insertBatch: atomicInsert, deleteBatch: atomicDelete, dtype: ISCPDataType_Tail,
			updateWatermark: func(context.Context, string, client.TxnOperator) error {
				watermarkUpdates++
				return nil
			},
		},
		from: types.BuildTS(10, 0), to: types.BuildTS(30, 0),
	}
	consumer := &MaterializedViewConsumer{
		cnUUID: service, jobID: JobID{DBName: "srcdb", TableName: "events"},
		info: &ConsumerInfo{
			DBName: "db", TableName: "mv", Columns: []string{"service", "requests"},
			SourceSQL: "events", RefreshSQL: "select service, count(*) requests from events group by service",
			IncrementalSpec: encodeMaterializedViewIncrementalDescription(t, desc), RefreshMethod: "fast",
			SrcTables: []TableInfo{{DBName: "srcdb", TableName: "events"}},
		},
	}
	catalog := newMVTestCatalog(t, consumer.info, tableDef)
	catalog.stubTransactions(t)
	consumer.cnEngine = catalog
	catalog.relations["srcdb.events"].rows = [][]any{{[]byte("api"), int64(5)}}
	require.NoError(t, consumer.Consume(t.Context(), retriever))
	require.Equal(t, 1, watermarkUpdates)
	require.Len(t, sqls, 3)
}

func TestMaterializedViewConsumerRoutesUnionAllTailBySource(t *testing.T) {
	const service = "materialized-view-consumer-union-tail-test"
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
	moruntime.SetupServiceBasedRuntime(service, rt)
	var sqls []string
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	}))
	stubTxn := stubIndexConsumerTxnRunner()
	t.Cleanup(stubTxn.Reset)

	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	insertBat := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(1, types.T_varchar.ToType(), mp, false, []string{"api"}),
	}, nil)
	insertBat.Attrs = []string{"service"}
	t.Cleanup(func() { insertBat.Clean(mp) })
	atomicInsert := NewAtomicBatch(mp)
	atomicInsert.Batches = []*batch.Batch{insertBat}
	atomicInsert.Rows.Set(AtomicBatchRow{Pk: []byte("insert"), Src: insertBat})

	branch := func(id int, db, table string) *incrementalDescription {
		return &incrementalDescription{
			Version: 3, Strategy: "direct-delta", BranchID: id,
			SourceDatabase: db, SourceTable: table, SourceAlias: "e", SourceColumns: []string{"service"},
			Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
			Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
			GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
			StateColumns: []string{"__row_count", "__group_key"},
		}
	}
	desc := incrementalDescription{
		Version: 3, Strategy: "union-all", SourceAlias: "__union__",
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"},
		Branches: []incrementalBranch{
			{Description: branch(1, "obs", "events")},
			{Description: branch(2, "cold", "archive")},
			{Description: branch(3, "obs", "events")},
		},
	}
	watermarkUpdates := 0
	retriever := &materializedViewBoundaryRetriever{
		MockRetriever: MockRetriever{
			insertBatch: atomicInsert, dtype: ISCPDataType_Tail, sourceTableID: 11,
			updateWatermark: func(context.Context, string, client.TxnOperator) error {
				watermarkUpdates++
				return nil
			},
		},
		from: types.BuildTS(10, 0), to: types.BuildTS(30, 0),
	}
	consumer := &MaterializedViewConsumer{
		cnUUID: service,
		info: &ConsumerInfo{
			DBName: "db", TableName: "mv", Columns: []string{"service", "requests"},
			SourceSQL: "events", RefreshSQL: "select service, count(*) requests from obs.events group by service union all select service, count(*) requests from cold.archive group by service",
			IncrementalSpec: encodeMaterializedViewIncrementalDescription(t, desc), RefreshMethod: "fast",
			SrcTables: []TableInfo{{DBName: "obs", TableName: "events", TableID: 11}, {DBName: "cold", TableName: "archive", TableID: 22}},
		},
	}
	catalog := newMVTestCatalog(t, consumer.info,
		&planpb.TableDef{DbName: "obs", Name: "events", TblId: 11, Cols: []*planpb.ColDef{{Name: "service", Typ: planpb.Type{Id: int32(types.T_varchar)}}}},
		&planpb.TableDef{DbName: "cold", Name: "archive", TblId: 22, Cols: []*planpb.ColDef{{Name: "service", Typ: planpb.Type{Id: int32(types.T_varchar)}}}})
	catalog.stubTransactions(t)
	consumer.cnEngine = catalog
	require.NoError(t, consumer.Consume(t.Context(), retriever))
	require.Equal(t, 1, watermarkUpdates)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "serial_full(1,d.__mo_g_0)")
	require.Contains(t, sqls[1], "serial_full(3,d.__mo_g_0)")
	for _, sql := range sqls {
		require.NotContains(t, sql, "serial_full(2,d.__mo_g_0)")
	}
}

func materializedViewRowsFromBatch(bat *AtomicBatch, insert bool) ([]materializedViewChangeRow, error) {
	var rows []materializedViewChangeRow
	err := visitMaterializedViewRows(context.Background(), bat, insert, nil, materializedViewDeltaBatchRows, materializedViewDeltaMaxSQL, func(chunk []materializedViewChangeRow) error {
		rows = append(rows, chunk...)
		return nil
	})
	return rows, err
}

func TestMaterializedViewRowVisitorBoundsCancellationAndErrors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := testutil.NewVector(3, types.T_varchar.ToType(), mp, false, []string{"a", "b", "c"})
	bat := testutil.NewBatchWithVectors([]*vector.Vector{vec}, nil)
	bat.Attrs = []string{"k"}
	defer func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()) }()
	atomic := NewAtomicBatch(mp)
	for i := range 3 {
		atomic.Rows.Set(AtomicBatchRow{Pk: []byte{byte(i)}, Src: bat, Offset: i})
	}
	var sizes []int
	var values []string
	err := visitMaterializedViewRows(t.Context(), atomic, true, map[string]bool{"k": true}, 2, 1024, func(rows []materializedViewChangeRow) error {
		sizes = append(sizes, len(rows))
		for _, row := range rows {
			values = append(values, string(row.Values["k"].([]byte)))
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int{2, 1}, sizes)
	require.Equal(t, []string{"a", "b", "c"}, values)
	visits := 0
	visit := func([]materializedViewChangeRow) error { visits++; return nil }
	err = visitMaterializedViewRows(t.Context(), atomic, true, nil, 10, 257, visit)
	require.ErrorIs(t, err, engine.ErrRowIDReadLimit)
	require.Zero(t, visits)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, visitMaterializedViewRows(ctx, atomic, true, nil, 2, 1024, visit), context.Canceled)
	require.Zero(t, visits)
	failure := errors.New("apply chunk failed")
	err = visitMaterializedViewRows(t.Context(), atomic, true, nil, 1, 1024, func([]materializedViewChangeRow) error { visits++; return failure })
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, visits)
}
func TestMaterializedViewTerminalFailuresNeverFallback(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, engine.ErrRowIDReadLimit, errMaterializedViewDeltaSQLTooLarge, mvdefinition.Invalid("replaced source"), &materializedViewNoFallback{error: errors.New("rollback failed")}} {
		require.False(t, materializedViewCanFallback(t.Context(), err), err)
	}
	require.True(t, materializedViewCanFallback(t.Context(), errors.New("recoverable delta SQL failure")))
}
