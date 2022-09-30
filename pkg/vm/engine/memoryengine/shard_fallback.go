// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type FallbackShard []ShardPolicy

var _ ShardPolicy = FallbackShard{}

func (f FallbackShard) Batch(ctx context.Context, tableID ID, getDefs func(context.Context) ([]engine.TableDef, error), batch *batch.Batch, nodes []logservicepb.DNStore) (sharded []*ShardedBatch, err error) {
	for _, policy := range f {
		sharded, err := policy.Batch(ctx, tableID, getDefs, batch, nodes)
		if err != nil {
			return nil, err
		}
		if len(sharded) == 0 {
			continue
		}
		return sharded, nil
	}
	panic("all shard policy failed")
}

func (f FallbackShard) Vector(ctx context.Context, tableID ID, getDefs func(context.Context) ([]engine.TableDef, error), colName string, vec *vector.Vector, nodes []logservicepb.DNStore) (sharded []*ShardedVector, err error) {
	for _, policy := range f {
		sharded, err := policy.Vector(ctx, tableID, getDefs, colName, vec, nodes)
		if err != nil {
			return nil, err
		}
		if len(sharded) == 0 {
			continue
		}
		return sharded, nil
	}
	panic("all shard policy failed")
}
