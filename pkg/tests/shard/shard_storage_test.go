// Copyright 2021 - 2024 Matrix Origin
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

package shard

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestInitSQLCanCreated(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			testutils.ExecSQL(
				t,
				catalog.MO_CATALOG,
				cn1,
				shardservice.InitSQLs...,
			)
		},
	)
}

func TestPartitionBasedShardCanBeCreated(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			testutils.ExecSQL(
				t,
				catalog.MO_CATALOG,
				cn1,
				shardservice.InitSQLs...,
			)

			db := testutils.GetDatabaseName(t)
			table := t.Name()
			sql := `
			CREATE TABLE %s (
				id          INT             NOT NULL,
				PRIMARY KEY (id)
			) PARTITION BY RANGE columns (id)(
				partition p01 values less than (10),
				partition p02 values less than (20),
				partition p03 values less than (30),
				partition p04 values less than (40)
			);
			`

			testutils.CreateTableAndWaitCNApplied(
				t,
				db,
				table,
				fmt.Sprintf(sql, table),
				cn1,
			)
		},
	)
}
