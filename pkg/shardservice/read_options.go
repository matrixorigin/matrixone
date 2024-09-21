// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"github.com/cespare/xxhash/v2"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func (opts ReadOptions) PK(
	pk []byte,
) ReadOptions {
	opts.hash = xxhash.Sum64(pk)
	return opts
}

func (opts ReadOptions) ReadAt(
	readAt timestamp.Timestamp,
) ReadOptions {
	opts.readAt = readAt
	return opts
}

func (opts ReadOptions) Shard(
	value uint64,
) ReadOptions {
	opts.shardID = value
	return opts
}

func (opts ReadOptions) Adjust(
	fn func(*pb.TableShard),
) ReadOptions {
	opts.adjust = fn
	return opts
}

func (opts ReadOptions) filter(
	metadata pb.ShardsMetadata,
	shard pb.TableShard,
	replica pb.ShardReplica,
) bool {
	if opts.shardID != 0 {
		return opts.shardID == shard.ShardID
	}
	if opts.hash > 0 {
		switch metadata.Policy {
		case pb.Policy_None:
			panic("cannot sharding on none policy")
		case pb.Policy_Partition:
			panic("cannot sharding pk on partition policy")
		case pb.Policy_Hash:
			return shard.ShardID == opts.hash%uint64(metadata.ShardsCount)
		}
	}
	return true
}
