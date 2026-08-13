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
		"select * from `db1`.`a`{MO_TS = '100-7'} as x join `db2`.`b`{MO_TS = '100-7'} as y on x.id = y.id",
		query)
}
