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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/stretchr/testify/require"
)

var (
	once            sync.Once
	shardingCluster embed.Cluster
	mu              sync.Mutex
)

func runShardClusterTest(
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
									sc.CN.HAKeeper.HeatbeatInterval.Duration = time.Second
								} else if op.ServiceType() == metadata.ServiceType_TN {
									if sc.TNCompatible != nil {
										sc.TNCompatible.ShardService.Enable = true
										sc.TNCompatible.ShardService.FreezeCNTimeout.Duration = time.Second
									}
									if sc.TN_please_use_getTNServiceConfig != nil {
										sc.TN_please_use_getTNServiceConfig.ShardService.Enable = true
										sc.TN_please_use_getTNServiceConfig.ShardService.FreezeCNTimeout.Duration = time.Second
									}
								} else if op.ServiceType() == metadata.ServiceType_LOG {
									sc.LogService.HAKeeperConfig.CNStoreTimeout.Duration = time.Second * 5
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
		value       INT             NULL,
		PRIMARY KEY (id)
	) PARTITION BY RANGE columns (id)(
		%s
	);
	`,
		tableName,
		partitionsDDL,
	)
}

func getLocalPartitionValue(
	t *testing.T,
	table uint64,
	cnIndex int,
	c embed.Cluster,
) int {
	cn, err := c.GetCNService(cnIndex)
	require.NoError(t, err)

	s := shardservice.GetService(cn.ServiceID())
	store := s.GetStorage()
	_, metadata, err := store.Get(table)
	require.NoError(t, err)

	_, _, _, err = s.GetShardInfo(table)
	require.NoError(t, err)

	for i, shardID := range metadata.ShardIDs {
		has, err := s.HasLocalReplica(table, shardID)
		require.NoError(t, err)

		if has {
			return i*10 + 1
		}
	}
	panic("no local shard found")
}

func getRemotePartitionValue(
	t *testing.T,
	table uint64,
	c embed.Cluster,
) []int {
	cn, err := c.GetCNService(0)
	require.NoError(t, err)

	s := shardservice.GetService(cn.ServiceID())
	store := s.GetStorage()
	_, metadata, err := store.Get(table)
	require.NoError(t, err)

	values := make(map[uint64]int)
	for i, shardID := range metadata.ShardIDs {
		values[shardID] = i*10 + 1
	}
	shards := make(map[int]uint64)

	for i := 0; i < 3; i++ {
		id := func(i int) uint64 {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			s = shardservice.GetService(cn.ServiceID())
			for _, shardID := range metadata.ShardIDs {
				has, err := s.HasLocalReplica(table, shardID)
				require.NoError(t, err)
				if has {
					return shardID
				}
			}
			panic("no local shard found")
		}(i)
		shards[i] = id
	}
	var result []int
	info := ""
	info += fmt.Sprintf(">>>>>>>>> shards-values: %v\n", values)
	info += fmt.Sprintf(">>>>>>>>> shards: %v\n", shards)
	selected := make(map[uint64]uint64)
	for i := 0; i < 3; i++ {
		local := shards[i]
		result = append(result,
			func(local uint64) int {
				for shard, value := range values {
					preSelected := selected[shard]
					if shard != local && local != preSelected {
						selected[local] = shard
						delete(values, shard)
						info += fmt.Sprintf(">>>>>>>>> %d select %d: %v\n", local, value, shards)
						return value
					}
				}
				panic("no remote shard found\n" + info)
			}(local),
		)
	}
	return result
}

func getAllPartitionValues(
	t *testing.T,
	table uint64,
	c embed.Cluster,
) []int {
	cn, err := c.GetCNService(0)
	require.NoError(t, err)

	s := shardservice.GetService(cn.ServiceID())
	store := s.GetStorage()
	_, metadata, err := store.Get(table)
	require.NoError(t, err)

	values := make([]int, 0, len(metadata.ShardIDs))
	for i := range metadata.ShardIDs {
		values = append(values, i*10+1)
	}
	return values
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

func waitReplicaCount(
	t *testing.T,
	c embed.Cluster,
	tableID uint64,
	replicas int64,
	cnIndexes []int,
) {
	for {
		n := int64(0)
		for _, idx := range cnIndexes {
			cn, err := c.GetCNService(idx)
			require.NoError(t, err)

			s := shardservice.GetService(cn.RawService().(cnservice.Service).ID())
			n += s.TableReplicaCount(tableID)
		}
		if n == replicas {
			return
		}
		time.Sleep(time.Second)
	}
}

func waitCNDown(
	sid string,
	cn string,
) {
	cs := clusterservice.GetMOCluster(sid)

	for {
		found := false
		cs.GetCNService(
			clusterservice.NewServiceIDSelector(cn),
			func(c metadata.CNService) bool {
				found = true
				return true
			},
		)
		if !found {
			return
		}
		time.Sleep(time.Second)
	}
}

func mustGetTNService(
	t *testing.T,
	c embed.Cluster,
) embed.ServiceOperator {
	var tn embed.ServiceOperator
	c.ForeachServices(
		func(s embed.ServiceOperator) bool {
			if s.ServiceType() == metadata.ServiceType_TN {
				tn = s
				return false
			}
			return true
		},
	)
	require.NotNil(t, tn)
	return tn
}
