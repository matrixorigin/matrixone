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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestReplicaBalance(t *testing.T) {
	runShardClusterTest(
		func(c embed.Cluster) {
			// 3 replicas must allocated to 3 cn
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			// cn1 down, and the replica which on cn1 must be allocated to other cns
			cn1, err := c.GetCNService(0)
			require.NoError(t, err)
			require.NoError(t, cn1.Close())
			tn := mustGetTNService(t, c)
			waitCNDown(tn.ServiceID(), cn1.ServiceID())
			waitReplicaCount(t, c, tableID, 3, []int{1, 2})

			// restart cn1, and the replicas must be balanced
			require.NoError(t, cn1.Start())
			waitReplica(t, c, tableID, []int64{1, 1, 1})
		},
	)
}
