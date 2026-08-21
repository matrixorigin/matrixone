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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestNewMaterializedViewConsumerValidatesSpec(t *testing.T) {
	_, err := NewMaterializedViewConsumer("", nil, nil, JobID{}, nil)
	require.Error(t, err)

	consumer, err := NewMaterializedViewConsumer("", nil, nil, JobID{}, &ConsumerInfo{
		DBName: "db", TableName: "mv", RefreshSQL: "select count(*) from src", SourceSQL: "src",
	})
	require.NoError(t, err)
	require.IsType(t, &MaterializedViewConsumer{}, consumer)
}

func TestMaterializedViewRefreshAtIterationBoundary(t *testing.T) {
	ts := types.BuildTS(100, 7)
	query, err := materializedViewRefreshAt("select src, count(*) from src group by src", "src", ts)
	require.NoError(t, err)
	require.Equal(t, "select src, count(*) from src{MO_TS = '100-7'} group by src", query)

	_, err = materializedViewRefreshAt("select 1", "src", ts)
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
