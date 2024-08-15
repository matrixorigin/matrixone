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
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			store1 := s1.storage.(*MemShardStorage)
			s2 := services[1]
			store2 := s2.storage.(*MemShardStorage)

			table := uint64(1)
			shards := uint32(2)
			mustAddTestShards(t, ctx, s1, table, shards, 1, s2)
			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)

			k := []byte("k")

			s1v1 := []byte("s1v1")
			s1v2 := []byte("s1v2")
			store1.set(k, s1v1, newTestTimestamp(1))
			store1.set(k, s1v2, newTestTimestamp(2))
			store1.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			s2v1 := []byte("s2v1")
			s2v2 := []byte("s2v2")
			store2.set(k, s2v1, newTestTimestamp(1))
			store2.set(k, s2v2, newTestTimestamp(2))
			store2.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			fn := func(
				s *service,
				ts timestamp.Timestamp,
				expectValues [][]byte,
			) {
				var values [][]byte
				err := s.Read(
					ctx,
					ReadRequest{
						TableID: table,
						Param:   shard.ReadParam{KeyParam: shard.KeyParam{Key: k}},
						Apply: func(b []byte) {
							values = append(values, b)
						},
					},
					DefaultOptions.ReadAt(ts),
				)
				require.NoError(t, err)

				sort.Slice(
					expectValues,
					func(i, j int) bool {
						return string(expectValues[i]) < string(expectValues[j])
					},
				)
				sort.Slice(
					values,
					func(i, j int) bool {
						return string(values[i]) < string(values[j])
					},
				)

				require.Equal(t, expectValues, values)
			}

			fn(s1, newTestTimestamp(2), [][]byte{s1v1, s2v1})
			fn(s1, newTestTimestamp(3), [][]byte{s1v2, s2v2})
			fn(s2, newTestTimestamp(2), [][]byte{s1v1, s2v1})
			fn(s2, newTestTimestamp(3), [][]byte{s1v2, s2v2})
		},
		nil,
	)
}

func TestReadWithSpecialShard(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			store1 := s1.storage.(*MemShardStorage)
			s2 := services[1]
			store2 := s2.storage.(*MemShardStorage)

			table := uint64(1)
			shards := uint32(2)
			mustAddTestShards(t, ctx, s1, table, shards, 1, s2)
			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)

			k := []byte("k")

			s1v1 := []byte("s1v1")
			s1v2 := []byte("s1v2")
			store1.set(k, s1v1, newTestTimestamp(1))
			store1.set(k, s1v2, newTestTimestamp(2))
			store1.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			s2v1 := []byte("s2v1")
			s2v2 := []byte("s2v2")
			store2.set(k, s2v1, newTestTimestamp(1))
			store2.set(k, s2v2, newTestTimestamp(2))
			store2.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			fn := func(
				s *service,
				shardID uint64,
				ts timestamp.Timestamp,
				expect []byte,
			) {
				err := s.Read(
					ctx,
					ReadRequest{
						TableID: table,
						Param:   shard.ReadParam{KeyParam: shard.KeyParam{Key: k}},
						Apply: func(b []byte) {
							require.Equal(t, expect, b)
						},
					},
					DefaultOptions.ReadAt(ts).Shard(shardID),
				)
				require.NoError(t, err)
			}

			shard1 := s1.getAllocatedShards()[0].ShardID
			shard2 := s2.getAllocatedShards()[0].ShardID

			fn(s1, shard1, newTestTimestamp(2), s1v1)
			fn(s1, shard2, newTestTimestamp(3), s2v2)
			fn(s2, shard1, newTestTimestamp(2), s1v1)
			fn(s2, shard2, newTestTimestamp(3), s2v2)
		},
		nil,
	)
}

func TestReadWithSpecialShardAndPartitionPolicy(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			store1 := s1.storage.(*MemShardStorage)
			s2 := services[1]
			store2 := s2.storage.(*MemShardStorage)

			table := uint64(1)
			shards := uint32(2)
			mustAddTestPartitionShards(t, ctx, s1, table, shards, 1, s2)
			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)

			k := []byte("k")

			s1v1 := []byte("s1v1")
			s1v2 := []byte("s1v2")
			store1.set(k, s1v1, newTestTimestamp(1))
			store1.set(k, s1v2, newTestTimestamp(2))
			store1.waiter.NotifyLatestCommitTS(newTestTimestamp(4))
			shard1 := s1.getAllocatedShards()[0].ShardID

			s2v1 := []byte("s2v1")
			s2v2 := []byte("s2v2")
			store2.set(k, s2v1, newTestTimestamp(1))
			store2.set(k, s2v2, newTestTimestamp(2))
			store2.waiter.NotifyLatestCommitTS(newTestTimestamp(4))
			shard2 := s2.getAllocatedShards()[0].ShardID

			fn := func(
				s *service,
				shardID uint64,
				ts timestamp.Timestamp,
				expect []byte,
			) {
				n := 0
				err := s.Read(
					ctx,
					ReadRequest{
						TableID: table,
						Param:   shard.ReadParam{KeyParam: shard.KeyParam{Key: k}},
						Apply: func(b []byte) {
							n++
							require.Equal(t, expect, b)
						},
					},
					DefaultOptions.ReadAt(ts).Shard(shardID),
				)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}

			fn(s1, shard1, newTestTimestamp(2), s1v1)
			fn(s1, shard2, newTestTimestamp(3), s2v2)
			fn(s2, shard1, newTestTimestamp(2), s1v1)
			fn(s2, shard2, newTestTimestamp(3), s2v2)
		},
		nil,
	)
}

func TestReadWithStaleReplica(t *testing.T) {
	runServicesTest(
		t,
		"cn1,cn2",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			s2 := services[1]

			table := uint64(1)
			shards := uint32(2)
			mustAddTestShards(t, ctx, s1, table, shards, 1, s2)
			waitReplicaCount(table, s1, 1)
			waitReplicaCount(table, s2, 1)

			fn := func(
				adjust func(shard *shard.TableShard),
			) error {
				return unwrapError(
					s1.Read(
						ctx,
						ReadRequest{
							TableID: table,
						},
						DefaultOptions.Shard(2).Adjust(
							func(src *shard.TableShard) {
								*src = src.Clone()
								adjust(src)
							},
						),
					),
				)
			}

			err := fn(
				func(shard *shard.TableShard) {
					shard.Replicas[0].ReplicaID = 10
				},
			)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrReplicaNotFound))
			require.False(t, s1.getReadCache().hasTableCache(table))

			err = fn(
				func(shard *shard.TableShard) {
					shard.Replicas[0].Version = 10
				},
			)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrReplicaNotMatch))
			require.False(t, s1.getReadCache().hasTableCache(table))
		},
		nil,
	)
}

func TestReadWithLazyCreateShards(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			store1 := s1.storage.(*MemShardStorage)

			table := uint64(1)
			shards := uint32(3)
			mustAddTestShards(t, ctx, s1, table, shards, 1)

			k := []byte("k")
			s1v1 := []byte("s1v1")
			s1v2 := []byte("s1v2")
			store1.set(k, s1v1, newTestTimestamp(1))
			store1.set(k, s1v2, newTestTimestamp(2))
			store1.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			fn := func(
				s *service,
				ts timestamp.Timestamp,
				expectValues [][]byte,
			) {
				var values [][]byte
				err := s.Read(
					ctx,
					ReadRequest{
						TableID: table,
						Param:   shard.ReadParam{KeyParam: shard.KeyParam{Key: k}},
						Apply: func(b []byte) {
							values = append(values, b)
						},
					},
					DefaultOptions.ReadAt(ts),
				)
				require.NoError(t, err)

				sort.Slice(
					expectValues,
					func(i, j int) bool {
						return string(expectValues[i]) < string(expectValues[j])
					},
				)
				sort.Slice(
					values,
					func(i, j int) bool {
						return string(values[i]) < string(values[j])
					},
				)

				require.Equal(t, expectValues, values)
			}

			fn(s1, newTestTimestamp(2), [][]byte{s1v1, s1v1, s1v1})
			fn(s1, newTestTimestamp(3), [][]byte{s1v2, s1v2, s1v2})
		},
		func(c *Config) []Option {
			return []Option{withDisableAppendCreateCallback()}
		},
	)
}

func TestReadWithNewShard(t *testing.T) {
	runServicesTest(
		t,
		"cn1",
		func(
			ctx context.Context,
			server *server,
			services []*service,
		) {
			s1 := services[0]
			store1 := s1.storage.(*MemShardStorage)
			table := uint64(1)
			shards := uint32(1)
			mustAddTestShards(t, ctx, s1, table, shards, 1)
			waitReplicaCount(table, s1, 1)

			store := s1.storage.(*MemShardStorage)
			store.Lock()
			v := store.committed[table]
			v.Version++
			v.ShardsCount = 2
			v.ShardIDs = []uint64{1, 2}
			store.committed[table] = v
			store.Unlock()

			k := []byte("k")
			value := []byte("v")

			store1.set(k, value, newTestTimestamp(1))
			store1.waiter.NotifyLatestCommitTS(newTestTimestamp(4))

			err := s1.Read(
				ctx,
				ReadRequest{
					TableID: table,
					Param:   shard.ReadParam{KeyParam: shard.KeyParam{Key: k}},
					Apply: func(b []byte) {
						require.Equal(t, value, b)
					},
				},
				DefaultOptions.ReadAt(newTestTimestamp(2)).Shard(2),
			)
			require.NoError(t, err)
			require.Equal(t, uint64(3), s1.atomic.added.Load())
			require.Equal(t, uint64(1), s1.atomic.removed.Load())
		},
		func(c *Config) []Option {
			c.CheckChangedDuration.Duration = time.Minute
			return nil
		},
	)
}

func newTestTimestamp(
	v int64,
) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: v,
	}
}

type joinError interface {
	Unwrap() []error
}

func unwrapError(
	err error,
) error {
	if e, ok := err.(joinError); ok {
		return e.Unwrap()[0]
	}
	return err
}
