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

package config

import (
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/storage"
)

type Config struct {
	CubeConfig    cConfig.Config //config of cubeDriver
	ClusterConfig ClusterConfig  //config of the cluster
	// FeaturesConfig config of all storages feature
	FeaturesConfig CubeFeatureConfig
}

type ClusterConfig struct {
	PreAllocatedGroupNum uint64 `toml:"pre-allocated-group-num"` //the number of shards allocated in initiation
	MaxGroupNum          uint64 `toml:"max-group-num"`           //the max number of shards
}

// CubeFeatureConfig MO will have multiple internal storage engines, each storage engine
// manages a group of shards, each group of shard splitting, log compaction and other
// features need to be independent. Here the configuration of all storage engine features
// managed by MO is configured.
type CubeFeatureConfig struct {
	// KV kv storage cube feature
	KV FeatureConfig `toml:"kv-feature"`
	// AOE aoe storage cube feature
	AOE FeatureConfig `toml:"aoe-feature"`
}

// FeatureConfig individual storage engine feature configuration
type FeatureConfig struct {
	// ShardSplitCheckDuration duration to check if the Shard needs to be split.
	ShardSplitCheckDuration typeutil.Duration `toml:"shard-split-check-duration"`
	// ShardCapacity the size of the data managed by each Shard, beyond which the Shard needs
	// to be split.
	ShardCapacityBytes typeutil.ByteSize `toml:"shard-capacity-bytes"`
	// ShardSplitCheckBytes the Cube records a shard-managed data size in memory, which is an approximate
	// value that changes after each Write call. Whenever this value exceeds the size set by the
	// current field, a real check is made to see if a split is needed, involving real IO operations.
	ShardSplitCheckBytes typeutil.ByteSize `toml:"shard-split-check-bytes"`
	// DisableShardSplit disable shard split
	DisableShardSplit bool `toml:"disable-shard-split"`
	// ForceCompactCount force compaction when the number of Raft logs reaches the specified number
	ForceCompactCount uint64 `toml:"force-compact-count"`
	// ForceCompactBytes force compaction when the number of Raft logs reaches the specified bytes
	ForceCompactBytes typeutil.ByteSize `toml:"force-compact-bytes"`
}

// Feature convert to storage.Feature
func (f *FeatureConfig) Feature() storage.Feature {
	return storage.Feature{
		ShardSplitCheckDuration: f.ShardSplitCheckDuration.Duration,
		ShardCapacityBytes:      uint64(f.ShardCapacityBytes),
		ShardSplitCheckBytes:    uint64(f.ShardSplitCheckBytes),
		DisableShardSplit:       f.DisableShardSplit,
		ForceCompactCount:       f.ForceCompactCount,
		ForceCompactBytes:       uint64(f.ForceCompactBytes),
	}
}
