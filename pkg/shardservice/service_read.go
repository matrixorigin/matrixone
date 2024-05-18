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

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func (s *service) Read(
	ctx context.Context,
	table uint64,
	payload []byte,
	apply func([]byte),
	opts ReadOptions,
) error {
	cache, err := s.getShards(table)
	if err != nil {
		return err
	}

	selected := newSlice()
	defer selected.close()

	cache.selectReplicas(
		table,
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

	for i, shard := range selected.values {
		if s.isLocalReplica(shard.Replicas[0]) {
			selected.local = append(selected.local, i)
			continue
		}

		if opts.adjust != nil {
			opts.adjust(&shard)
		}
		f, e := s.remote.client.AsyncSend(
			ctx,
			s.newReadRequest(
				shard,
				payload,
				opts.readAt,
			),
		)
		if e != nil {
			err = errors.Join(err, e)
			continue
		}
		futures.values = append(futures.values, f)
	}

	for _, i := range selected.local {
		if opts.adjust != nil {
			opts.adjust(&selected.values[i])
		}

		v, e := s.doRead(
			ctx,
			selected.values[i],
			opts.readAt,
			payload,
		)
		if e == nil {
			apply(v)
		}
		s.maybeRemoveReadCache(table, e)
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
			apply(resp.ShardRead.Payload)
			s.remote.pool.ReleaseResponse(resp)
		} else {
			s.maybeRemoveReadCache(table, e)
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
	payload []byte,
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
		shard.TableID,
		payload,
		readAt,
	)
}
