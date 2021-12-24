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
	"time"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixone/pkg/chaostesting"
)

type RandomizeCubeConfig func(config *config.Config)

func (_ Def) RandomizeCubeConfig() RandomizeCubeConfig {
	return func(config *config.Config) {

		fz.RandBetween(&config.Capacity, 1*1024*1024, 1*1024*1024*1024)
		fz.RandBetween(&config.ShardGroups, 3, 7)
		fz.RandBetween(&config.Replication.MaxPeerDownTime.Duration,
			time.Minute, time.Minute*5)
		fz.RandBetween(&config.Replication.ShardHeartbeatDuration.Duration,
			time.Minute, time.Minute*5)
		fz.RandBetween(&config.Replication.StoreHeartbeatDuration.Duration,
			time.Minute, time.Minute*5)
		fz.RandBetween(&config.Replication.ShardSplitCheckDuration.Duration,
			time.Minute, time.Minute*5)
		fz.RandBetween(&config.Replication.ShardStateCheckDuration.Duration,
			time.Minute, time.Minute*5)
		fz.RandBetween(&config.Replication.CompactLogCheckDuration.Duration,
			time.Minute, time.Minute*5)
		fz.RandBool(&config.Replication.DisableShardSplit)
		fz.RandBool(&config.Replication.AllowRemoveLeader)
		fz.RandBetween(&config.Replication.ShardCapacityBytes, 4*1024*1024, 128*1024*1024)
		fz.RandBetween(&config.Replication.ShardSplitCheckBytes, 4*1024*1024, 128*1024*1024)

		//TODO more fields

	}
}
