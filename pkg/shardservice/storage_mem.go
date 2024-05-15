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
	"sync"
	"sync/atomic"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type MemShardStorage struct {
	sync.RWMutex
	count struct {
		unsubscribe map[uint64]int
	}
	committed         map[uint64]pb.ShardsMetadata
	uncommittedAdd    map[uint64]pb.ShardsMetadata
	uncommittedDelete map[uint64]struct{}

	id atomic.Uint64
}

func NewMemShardStorage() ShardStorage {
	s := &MemShardStorage{
		committed:         make(map[uint64]pb.ShardsMetadata),
		uncommittedAdd:    make(map[uint64]pb.ShardsMetadata),
		uncommittedDelete: make(map[uint64]struct{}),
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
) (pb.ShardsMetadata, error) {
	s.RLock()
	defer s.RUnlock()
	return s.committed[table], nil
}

func (s *MemShardStorage) Create(
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
			if txn.Committed() {
				s.committed[table] = v
			}
			delete(s.uncommittedAdd, table)
		},
	)
	return true, nil
}

func (s *MemShardStorage) Delete(
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

func (s *MemShardStorage) UncommittedAdd(
	tableID uint64,
	value pb.ShardsMetadata,
) {
	s.Lock()
	defer s.Unlock()

	if value.Policy == pb.Policy_Partition {
		for i := 0; i < int(value.ShardsCount); i++ {
			value.PhysicalShardIDs = append(value.PhysicalShardIDs, s.id.Add(1))
		}
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
