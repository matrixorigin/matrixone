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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

var (
	once            sync.Once
	shardingCluster embed.Cluster
	mu              sync.Mutex
)

func runShardClusterTest(
	t *testing.T,
	fn func(embed.Cluster),
) error {
	mu.Lock()
	defer mu.Unlock()

	var err error
	var c embed.Cluster
	once.Do(
		func() {
			c, err = embed.NewCluster(
				embed.WithCNCount(3),
				embed.WithTesting(),
				embed.WithPreStart(
					func(op embed.ServiceOperator) {
						op.Adjust(
							func(sc *embed.ServiceConfig) {
								if op.ServiceType() == metadata.ServiceType_CN {
									sc.CN.ShardService.Enable = true
								} else if op.ServiceType() == metadata.ServiceType_TN {
									if sc.TNCompatible != nil {
										sc.TNCompatible.ShardService.Enable = true
									}
									if sc.TN_please_use_getTNServiceConfig != nil {
										sc.TN_please_use_getTNServiceConfig.ShardService.Enable = true
									}
								}
							},
						)
					},
				),
			)
			if err != nil {
				return
			}
			err = c.Start()
			if err != nil {
				return
			}
			shardingCluster = c

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)
			committedAt := initShardsOnce(t, cn1)
			testutils.WaitClusterAppliedTo(t, c, committedAt)
		},
	)
	if err != nil {
		return err
	}

	fn(shardingCluster)
	return nil
}

func getPartitionTableSQL(
	tableName string,
	partitions int,
) string {
	partitionsDDL := ""
	for i := 1; i <= partitions; i++ {
		partitionsDDL += fmt.Sprintf("partition p%d values less than (%d)", i, i*10)
		if i != partitions {
			partitionsDDL += ",\n"
		}
	}

	return fmt.Sprintf(
		`
	CREATE TABLE %s (
		id          INT             NOT NULL,
		PRIMARY KEY (id)
	) PARTITION BY RANGE columns (id)(
		%s
	);
	`,
		tableName,
		partitionsDDL,
	)
}
