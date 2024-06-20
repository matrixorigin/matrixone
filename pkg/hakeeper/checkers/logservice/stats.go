// Copyright 2021 - 2022 Matrix Origin
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

package logservice

type replica struct {
	// uuid is the store UUID
	uuid    string
	shardID uint64
	epoch   uint64

	replicaID uint64
}

// stats collects all replicas that need to be processed.
type stats struct {
	// zombies collects replicas that are already removed in config but still
	// running on log stores.
	zombies []replica

	// toStart collects replicas that are already added in config but
	// not running on log stores.
	toStart []replica

	// toRemove collects replicas that needs to be removed in config.
	// The key is shardID and the value is the slice of replicas.
	toRemove map[uint64][]replica

	// toAdd collects replicas that needs to be added in config.
	// The key is shardID and the value is the number of replicas to be added.
	toAdd map[uint64]uint32

	// nonVoting indicates if it is a stats for non-voting replicas.
	nonVoting bool
}

func newStats(nonVoting bool) *stats {
	return &stats{
		toRemove:  make(map[uint64][]replica),
		toAdd:     make(map[uint64]uint32),
		nonVoting: nonVoting,
	}
}
