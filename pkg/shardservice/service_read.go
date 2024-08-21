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
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func (s *service) Read(
	ctx context.Context,
	req ReadRequest,
	opts ReadOptions,
) error {
	var cache *readCache
	var err error
	for {
		cache, err = s.getShards(req.TableID)
		if err != nil {
			return err
		}
		if opts.shardID == 0 ||
			cache.hasShard(req.TableID, opts.shardID) {
			break
		}

		// remove old read cache
		s.removeReadCache(req.TableID)

		// shards updated, create new allocated
		s.createC <- req.TableID

		// wait shard created
		err = s.waitShardCreated(
			ctx,
			req.TableID,
			opts.shardID,
		)
		if err != nil {
			return err
		}
	}

	selected := newSlice()
	defer selected.close()

	cache.selectReplicas(
		req.TableID,
		func(
			metadata pb.ShardsMetadata,
			shard pb.TableShard,
			replica pb.ShardReplica,
		) bool {
			if opts.filter(metadata, shard, replica) {
				shard.Replicas = []pb.ShardReplica{replica}
				selected.values = append(selected.values, shard)
			}
			return true
		},
	)

	futures := newFutureSlice()
	defer futures.close()

	local := 0
	remote := 0
	for i, shard := range selected.values {
		if s.isLocalReplica(shard.Replicas[0]) {
			selected.local = append(selected.local, i)
			local++
			continue
		}

		remote++
		if opts.adjust != nil {
			opts.adjust(&shard)
		}
		f, e := s.remote.client.AsyncSend(
			ctx,
			s.newReadRequest(
				shard,
				req.Method,
				req.Param,
				opts.readAt,
			),
		)
		if e != nil {
			err = errors.Join(err, e)
			continue
		}
		futures.values = append(futures.values, f)
	}

	v2.ReplicaLocalReadCounter.Add(float64(local))
	v2.ReplicaRemoteReadCounter.Add(float64(remote))

	var buffer *morpc.Buffer
	for _, i := range selected.local {
		if opts.adjust != nil {
			opts.adjust(&selected.values[i])
		}

		if buffer == nil {
			buffer = morpc.NewBuffer()
			defer buffer.Close()
		}

		v, e := s.doRead(
			ctx,
			selected.values[i],
			opts.readAt,
			req.Method,
			req.Param,
			buffer,
		)
		if e == nil {
			req.Apply(v)
			continue
		}
		s.maybeRemoveReadCache(req.TableID, e)
		err = errors.Join(err, e)
		continue
	}

	var resp *pb.Response
	for _, f := range futures.values {
		v, e := f.Get()
		if e == nil {
			resp = v.(*pb.Response)
			resp, e = s.unwrapError(resp, e)
		}
		if e == nil {
			req.Apply(resp.ShardRead.Payload)
			s.remote.pool.ReleaseResponse(resp)
		} else {
			s.maybeRemoveReadCache(req.TableID, e)
			err = errors.Join(err, e)
		}

		f.Close()
	}
	return err
}

func (s *service) doRead(
	ctx context.Context,
	shard pb.TableShard,
	readAt timestamp.Timestamp,
	method int,
	param pb.ReadParam,
	buffer *morpc.Buffer,
) ([]byte, error) {
	if err := s.validReplica(
		shard,
		shard.Replicas[0],
	); err != nil {
		return nil, err
	}

	if err := s.storage.WaitLogAppliedAt(
		ctx,
		readAt,
	); err != nil {
		return nil, err
	}

	return s.storage.Read(
		ctx,
		shard,
		method,
		param,
		readAt,
		buffer,
	)
}
