// Copyright 2021 Matrix Origin
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

package main

import (
	"fmt"
	"time"

	prophetconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/metric"
)

type DefaultCubeConfig func(id int) *config.Config

func (_ Def) DefaultCubeConfig() DefaultCubeConfig {
	return func(i int) *config.Config {
		return &config.Config{

			DeployPath: "",
			Version:    "42",
			GitHash:    "",
			Labels: [][]string{
				{"node", fmt.Sprintf("%d", i)},
			},
			Capacity:           128 * 1024 * 1024,
			UseMemoryAsStorage: false,
			ShardGroups:        uint64(3),

			Replication: config.ReplicationConfig{
				MaxPeerDownTime:         typeutil.NewDuration(time.Minute * 3),
				ShardHeartbeatDuration:  typeutil.NewDuration(time.Millisecond * 100),
				StoreHeartbeatDuration:  typeutil.NewDuration(time.Millisecond * 100),
				ShardSplitCheckDuration: typeutil.NewDuration(time.Millisecond * 100),
				ShardStateCheckDuration: typeutil.NewDuration(time.Millisecond * 100),
				CompactLogCheckDuration: typeutil.NewDuration(time.Millisecond * 100),
				DisableShardSplit:       false,
				AllowRemoveLeader:       false,
				ShardCapacityBytes:      16 * 1024 * 1024,
				ShardSplitCheckBytes:    16 * 1024 * 1024,
			},

			Raft: config.RaftConfig{
				TickInterval:         typeutil.NewDuration(time.Millisecond * 100),
				HeartbeatTicks:       10,
				ElectionTimeoutTicks: 50,
				MaxSizePerMsg:        8 * 1024 * 1024,
				MaxInflightMsgs:      256,
				MaxEntryBytes:        1 * 1024 * 1024,
				SendRaftBatchSize:    128,

				RaftLog: config.RaftLogConfig{
					DisableSync:         false,
					CompactThreshold:    256,
					MaxAllowTransferLag: 4,
					ForceCompactCount:   2048,
					ForceCompactBytes:   128 * 1024 * 1024,
				},
			},

			Worker: config.WorkerConfig{
				RaftEventWorkers:       128,
				ApplyWorkerCount:       128,
				SendRaftMsgWorkerCount: 128,
			},

			Prophet: prophetconfig.Config{
				Name:       fmt.Sprintf("prophet-%d", i),
				RPCTimeout: typeutil.NewDuration(time.Second * 32),

				StorageNode:  true,
				ExternalEtcd: []string{},
				EmbedEtcd: prophetconfig.EmbedEtcdConfig{
					AdvertiseClientUrls:     "",
					AdvertisePeerUrls:       "",
					InitialCluster:          "",
					InitialClusterState:     "",
					TickInterval:            typeutil.NewDuration(time.Millisecond * 30),
					ElectionInterval:        typeutil.NewDuration(time.Millisecond * 150),
					PreVote:                 true,
					AutoCompactionMode:      "periodic",
					AutoCompactionRetention: "1h",
					QuotaBackendBytes:       1 * 1024 * 1024 * 1024,
				},

				LeaderLease: 8,

				Schedule: prophetconfig.ScheduleConfig{
					MaxSnapshotCount:              3,
					MaxPendingPeerCount:           16,
					MaxMergeResourceSize:          128 * 1024 * 1024,
					MaxMergeResourceKeys:          16,
					SplitMergeInterval:            typeutil.NewDuration(time.Minute),
					EnableOneWayMerge:             true,
					EnableCrossTableMerge:         true,
					PatrolResourceInterval:        typeutil.NewDuration(time.Minute),
					MaxContainerDownTime:          typeutil.NewDuration(time.Minute),
					LeaderScheduleLimit:           4,
					LeaderSchedulePolicy:          "count",
					ResourceScheduleLimit:         2048,
					ReplicaScheduleLimit:          64,
					MergeScheduleLimit:            128,
					HotResourceScheduleLimit:      128,
					HotResourceCacheHitsThreshold: 128,
					TolerantSizeRatio:             0.8,
					LowSpaceRatio:                 0.8,
					HighSpaceRatio:                0.2,
					EnableJointConsensus:          true,
					// ... TODO

				},

				Replication: prophetconfig.ReplicationConfig{
					MaxReplicas:          3,
					EnablePlacementRules: true,
				},
			},

			Metric: metric.Cfg{
				Addr:     "",
				Interval: 0,
			},
		}
	}
}
