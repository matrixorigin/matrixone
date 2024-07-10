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
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
)

type MemShardStorage struct {
	sync.RWMutex
	count struct {
		unsubscribe map[uint64]int
	}
	id                atomic.Uint64
	committed         map[uint64]pb.ShardsMetadata
	uncommittedAdd    map[uint64]pb.ShardsMetadata
	uncommittedDelete map[uint64]struct{}
	kv                *mem.KV
	waiter            client.TimestampWaiter
}

func NewMemShardStorage() ShardStorage {
	s := &MemShardStorage{
		committed:         make(map[uint64]pb.ShardsMetadata),
		uncommittedAdd:    make(map[uint64]pb.ShardsMetadata),
		uncommittedDelete: make(map[uint64]struct{}),
		kv:                mem.NewKV(),
		waiter:            client.NewTimestampWaiter(),
	}
	s.id.Store(1000000000)
	s.count.unsubscribe = make(map[uint64]int)
	return s
}

func (s *MemShardStorage) Unsubscribe(
	tables ...uint64,
) error {
	s.Lock()
	defer s.Unlock()
	for _, table := range tables {
		s.count.unsubscribe[table]++
	}
	return nil
}

func (s *MemShardStorage) Get(
	table uint64,
) (uint64, pb.ShardsMetadata, error) {
	s.RLock()
	defer s.RUnlock()
	m, ok := s.committed[table]
	if ok {
		return table, m, nil
	}

	for tid, m := range s.uncommittedAdd {
		if m.Policy != pb.Policy_Partition {
			continue
		}
		for _, sid := range m.ShardIDs {
			if sid == table {
				return tid, m, nil
			}
		}
	}
	return 0, pb.ShardsMetadata{}, nil
}

func (s *MemShardStorage) GetChanged(
	tables map[uint64]uint32,
	applyDeleted func(uint64),
	applyChanged func(uint64),
) error {
	s.RLock()
	defer s.RUnlock()

	for table, version := range tables {
		new, ok := s.committed[table]
		if !ok {
			applyDeleted(table)
			continue
		}
		if new.Version > version {
			applyChanged(table)
		}
	}
	return nil
}

func (s *MemShardStorage) Create(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) (bool, error) {
	s.Lock()
	defer s.Unlock()
	v := s.uncommittedAdd[table]
	if v.Policy == pb.Policy_None {
		return false, nil
	}
	txnOp.AppendEventCallback(
		client.ClosedEvent,
		func(txn client.TxnEvent) {
			s.Lock()
			defer s.Unlock()

			if txn.Committed() {
				s.committed[table] = v
			}
			delete(s.uncommittedAdd, table)
		},
	)
	return true, nil
}

func (s *MemShardStorage) Delete(
	ctx context.Context,
	table uint64,
	txnOp client.TxnOperator,
) (bool, error) {
	s.Lock()
	defer s.Unlock()

	v, ok := s.uncommittedAdd[table]
	if !ok {
		v, ok = s.committed[table]
	}

	if !ok || v.Policy == pb.Policy_None {
		return false, nil
	}

	s.uncommittedDelete[table] = struct{}{}
	txnOp.AppendEventCallback(
		client.ClosedEvent,
		func(txn client.TxnEvent) {
			s.Lock()
			defer s.Unlock()

			if txn.Committed() {
				delete(s.committed, table)
			}
			delete(s.uncommittedDelete, table)
		},
	)
	return true, nil
}

func (s *MemShardStorage) UnsubscribeCount(
	table uint64,
) int {
	s.RLock()
	defer s.RUnlock()
	return s.count.unsubscribe[table]
}

func (s *MemShardStorage) WaitLogAppliedAt(
	ctx context.Context,
	ts timestamp.Timestamp,
) error {
	_, err := s.waiter.GetTimestamp(ctx, ts)
	return err
}

func (s *MemShardStorage) Read(
	ctx context.Context,
	shard pb.TableShard,
	method int,
	param pb.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()

	var value []byte
	key := newKey(param.KeyParam.Key, ts)
	s.kv.DescendRange(
		key,
		func(
			k, v []byte,
		) bool {
			if bytes.Equal(k, key) {
				return true
			}
			if !bytes.Equal(param.KeyParam.Key, k[:len(k)-12]) {
				return false
			}

			value = v
			return false
		},
	)
	return value, nil
}

func (s *MemShardStorage) UncommittedAdd(
	tableID uint64,
	value pb.ShardsMetadata,
) {
	s.Lock()
	defer s.Unlock()

	if len(value.ShardIDs) != int(value.ShardsCount) {
		panic("shard count and shard ids not match")
	}
	s.uncommittedAdd[tableID] = value
}

func (s *MemShardStorage) AddCommitted(
	tableID uint64,
	value pb.ShardsMetadata,
) {
	s.Lock()
	defer s.Unlock()
	s.committed[tableID] = value
}

func (s *MemShardStorage) set(
	key, value []byte,
	ts timestamp.Timestamp,
) {
	s.Lock()
	defer s.Unlock()

	s.kv.Set(newKey(key, ts), value)
	s.waiter.NotifyLatestCommitTS(ts)
}

func newKey(
	key []byte,
	ts timestamp.Timestamp,
) []byte {
	newKey := make([]byte, len(key)+12)
	copy(newKey, key)
	buf.Int64ToBytesTo(ts.PhysicalTime, newKey[len(key):])
	buf.Uint32ToBytesTo(ts.LogicalTime, newKey[len(key)+8:])
	return newKey
}
