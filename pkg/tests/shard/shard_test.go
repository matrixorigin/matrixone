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
	return runShardClusterTestWithReuse(
		fn,
		true,
	)
}

func runShardClusterTestWithReuse(
	fn func(embed.Cluster),
	reuse bool,
) error {
	mu.Lock()
	defer mu.Unlock()

	var err error
	var cluster embed.Cluster
	createFunc := func() {
		c, err := embed.NewCluster(
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
		cluster = c
		if reuse {
			shardingCluster = cluster
		}
	}

	if err != nil {
		return err
	}

	if reuse {
		once.Do(createFunc)
		cluster = shardingCluster
	} else {
		createFunc()
	}

	fn(cluster)
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
	checkCNs := make([]shardservice.ShardService, 0, len(replicas))
	for i := range replicas {
		cn, err := c.GetCNService(i)
		require.NoError(t, err)

		checkCNs = append(
			checkCNs,
			shardservice.GetService(cn.RawService().(cnservice.Service).ID()))
	}

	for {
		matches := 0
		for i, v := range replicas {
			if v == checkCNs[i].TableReplicaCount(tableID) {
				matches++
			}
		}

		if matches == len(replicas) {
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

func TestA(t *testing.T) {
	data := []byte{103, 111, 114, 111, 117, 116, 105, 110, 101, 32, 52, 57, 49, 56, 49, 32, 91, 114, 117, 110, 110, 105, 110, 103, 93, 58, 10, 114, 117, 110, 116, 105, 109, 101, 47, 100, 101, 98, 117, 103, 46, 83, 116, 97, 99, 107, 40, 41, 10, 9, 47, 117, 115, 114, 47, 108, 111, 99, 97, 108, 47, 103, 111, 47, 115, 114, 99, 47, 114, 117, 110, 116, 105, 109, 101, 47, 100, 101, 98, 117, 103, 47, 115, 116, 97, 99, 107, 46, 103, 111, 58, 50, 54, 32, 43, 48, 120, 53, 101, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 101, 114, 114, 46, 67, 111, 110, 118, 101, 114, 116, 71, 111, 69, 114, 114, 111, 114, 40, 123, 48, 120, 53, 53, 57, 54, 50, 49, 48, 44, 32, 48, 120, 56, 50, 49, 51, 53, 99, 48, 125, 44, 32, 123, 48, 120, 53, 53, 52, 102, 98, 48, 48, 44, 32, 48, 120, 55, 102, 101, 51, 55, 101, 48, 125, 41, 10, 9, 47, 103, 111, 47, 115, 114, 99, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 101, 114, 114, 47, 101, 114, 114, 111, 114, 46, 103, 111, 58, 54, 54, 53, 32, 43, 48, 120, 49, 49, 57, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 112, 98, 47, 115, 104, 97, 114, 100, 46, 40, 42, 82, 101, 115, 112, 111, 110, 115, 101, 41, 46, 87, 114, 97, 112, 69, 114, 114, 111, 114, 40, 48, 120, 99, 48, 53, 53, 57, 99, 98, 51, 56, 48, 44, 32, 123, 48, 120, 53, 53, 52, 102, 98, 48, 48, 63, 44, 32, 48, 120, 55, 102, 101, 51, 55, 101, 48, 63, 125, 41, 10, 9, 47, 103, 111, 47, 115, 114, 99, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 112, 98, 47, 115, 104, 97, 114, 100, 47, 115, 104, 97, 114, 100, 46, 103, 111, 58, 49, 53, 48, 32, 43, 48, 120, 51, 97, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 46, 40, 42, 104, 97, 110, 100, 108, 101, 70, 117, 110, 99, 67, 116, 120, 91, 46, 46, 46, 93, 41, 46, 99, 97, 108, 108, 40, 48, 120, 53, 53, 54, 56, 49, 52, 48, 44, 32, 123, 48, 120, 53, 53, 57, 54, 51, 98, 56, 44, 32, 48, 120, 99, 48, 57, 99, 98, 98, 99, 50, 49, 48, 63, 125, 44, 32, 48, 120, 99, 48, 55, 51, 97, 55, 99, 101, 97, 48, 63, 44, 32, 48, 120, 99, 48, 53, 53, 57, 99, 98, 51, 56, 48, 63, 44, 32, 48, 120, 49, 48, 48, 48, 48, 99, 48, 48, 55, 97, 57, 48, 54, 56, 56, 41, 10, 9, 47, 103, 111, 47, 115, 114, 99, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 47, 109, 101, 116, 104, 111, 100, 95, 98, 97, 115, 101, 100, 46, 103, 111, 58, 55, 49, 32, 43, 48, 120, 55, 51, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 46, 40, 42, 109, 101, 116, 104, 111, 100, 66, 97, 115, 101, 100, 83, 101, 114, 118, 101, 114, 91, 46, 46, 46, 93, 41, 46, 111, 110, 77, 101, 115, 115, 97, 103, 101, 46, 102, 117, 110, 99, 49, 40, 41, 10, 9, 47, 103, 111, 47, 115, 114, 99, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 47, 109, 101, 116, 104, 111, 100, 95, 98, 97, 115, 101, 100, 46, 103, 111, 58, 50, 49, 51, 32, 43, 48, 120, 50, 99, 101, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 46, 40, 42, 109, 101, 116, 104, 111, 100, 66, 97, 115, 101, 100, 83, 101, 114, 118, 101, 114, 91, 46, 46, 46, 93, 41, 46, 111, 110, 77, 101, 115, 115, 97, 103, 101, 46, 102, 117, 110, 99, 50, 40, 41, 10, 9, 47, 103, 111, 47, 115, 114, 99, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 109, 97, 116, 114, 105, 120, 111, 114, 105, 103, 105, 110, 47, 109, 97, 116, 114, 105, 120, 111, 110, 101, 47, 112, 107, 103, 47, 99, 111, 109, 109, 111, 110, 47, 109, 111, 114, 112, 99, 47, 109, 101, 116, 104, 111, 100, 95, 98, 97, 115, 101, 100, 46, 103, 111, 58, 50, 50, 53, 32, 43, 48, 120, 52, 54, 10, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 112, 97, 110, 106, 102, 50, 48, 48, 48, 47, 97, 110, 116, 115, 47, 118, 50, 46, 40, 42, 103, 111, 87, 111, 114, 107, 101, 114, 41, 46, 114, 117, 110, 46, 102, 117, 110, 99, 49, 40, 41, 10, 9, 47, 103, 111, 47, 112, 107, 103, 47, 109, 111, 100, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 112, 97, 110, 106, 102, 50, 48, 48, 48, 47, 97, 110, 116, 115, 47, 118, 50, 64, 118, 50, 46, 55, 46, 52, 47, 119, 111, 114, 107, 101, 114, 46, 103, 111, 58, 54, 55, 32, 43, 48, 120, 56, 97, 10, 99, 114, 101, 97, 116, 101, 100, 32, 98, 121, 32, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 112, 97, 110, 106, 102, 50, 48, 48, 48, 47, 97, 110, 116, 115, 47, 118, 50, 46, 40, 42, 103, 111, 87, 111, 114, 107, 101, 114, 41, 46, 114, 117, 110, 32, 105, 110, 32, 103, 111, 114, 111, 117, 116, 105, 110, 101, 32, 52, 57, 53, 55, 49, 10, 9, 47, 103, 111, 47, 112, 107, 103, 47, 109, 111, 100, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 112, 97, 110, 106, 102, 50, 48, 48, 48, 47, 97, 110, 116, 115, 47, 118, 50, 64, 118, 50, 46, 55, 46, 52, 47, 119, 111, 114, 107, 101, 114, 46, 103, 111, 58, 52, 56, 32, 43, 48, 120, 53, 99, 10}
	fmt.Println(string(data))
	t.Fail()
}
