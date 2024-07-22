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
	"fmt"
	"strings"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

func tableShardsField(
	name string,
	shards pb.ShardsMetadata,
) zap.Field {
	return zap.String(name,
		fmt.Sprintf("policy: %s, count: %d, version: %d",
			shards.Policy,
			shards.ShardsCount,
			shards.Version,
		))
}

func tableShardSliceField(
	name string,
	shards []pb.TableShard,
) zap.Field {
	values := make([]string, 0, len(shards))
	for _, s := range shards {
		values = append(values, s.String())
	}
	return zap.String(name, strings.Join(values, " "))
}
