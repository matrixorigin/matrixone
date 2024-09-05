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
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPartitionBasedTableCanBeCreated(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})
		},
	)
}

func TestPartitionBasedTableCanBeDeleted(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			testutils.ExecSQL(
				t,
				db,
				cn1,
				fmt.Sprintf("drop table %s", t.Name()),
			)

			waitReplica(t, c, tableID, []int64{0, 0, 0})
		},
	)
}

func mustCreatePartitionBasedTable(
	t *testing.T,
	c embed.Cluster,
	db string,
	partitions int,
) uint64 {
	cn1, err := c.GetCNService(0)
	require.NoError(t, err)

	testutils.CreateTestDatabase(t, db, cn1)

	committedAt := testutils.ExecSQL(
		t,
		db,
		cn1,
		getPartitionTableSQL(t.Name(), partitions),
	)
	testutils.WaitClusterAppliedTo(t, c, committedAt)

	shardTableID := mustGetTableIDByCN(t, db, t.Name(), cn1)

	// check shard metadata created
	s1 := shardservice.GetService(cn1.RawService().(cnservice.Service).ID())
	store := s1.GetStorage()

	checkPartitionBasedShardMetadata(
		t,
		store,
		shardTableID,
		partitions,
	)

	return shardTableID
}

func waitReplica(
	t *testing.T,
	c embed.Cluster,
	tableID uint64,
	replicas []int64,
) {
	cn1, err := c.GetCNService(0)
	require.NoError(t, err)
	cn2, err := c.GetCNService(1)
	require.NoError(t, err)
	cn3, err := c.GetCNService(2)
	require.NoError(t, err)

	s1 := shardservice.GetService(cn1.RawService().(cnservice.Service).ID())
	s2 := shardservice.GetService(cn2.RawService().(cnservice.Service).ID())
	s3 := shardservice.GetService(cn3.RawService().(cnservice.Service).ID())

	for {
		n1 := s1.TableReplicaCount(tableID)
		n2 := s2.TableReplicaCount(tableID)
		n3 := s3.TableReplicaCount(tableID)
		if n1 == replicas[0] &&
			n2 == replicas[1] &&
			n3 == replicas[2] {
			return
		}
		time.Sleep(time.Second)
	}
}
