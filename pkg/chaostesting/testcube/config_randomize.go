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
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

type RandomizeCubeConfig func(config *config.Config)

func (_ Def) RandomizeCubeConfig() RandomizeCubeConfig {
	return func(config *config.Config) {

		// replication
		fz.RandBetween(&config.Replication.MaxPeerDownTime.Duration,
			time.Second*3, time.Second*5)
		fz.RandBetween(&config.Replication.ShardHeartbeatDuration.Duration,
			time.Second*1, time.Second*2)
		fz.RandBetween(&config.Replication.StoreHeartbeatDuration.Duration,
			time.Second*1, time.Second*2)
		fz.RandBetween(&config.Replication.ShardStateCheckDuration.Duration,
			time.Second*1, time.Second*2)
		fz.RandBetween(&config.Replication.CompactLogCheckDuration.Duration,
			time.Second*1, time.Second*2)

		// raft
		fz.RandBetween(&config.Raft.TickInterval.Duration,
			time.Millisecond*50, time.Millisecond*500)
		fz.RandBetween(&config.Raft.HeartbeatTicks,
			1, 10)
		fz.RandBetween(&config.Raft.ElectionTimeoutTicks,
			20, 40)
		fz.RandBetween(&config.Raft.RaftLog.CompactThreshold,
			1, 10)

		// worker
		fz.RandBetween(&config.Worker.RaftEventWorkers,
			1, 10)

	}
}
