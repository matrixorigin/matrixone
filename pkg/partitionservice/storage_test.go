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

package partitionservice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/require"
)

type memStorage struct {
	sync.RWMutex
	id          uint64
	committed   map[uint64]*partitionTable
	uncommitted map[uint64]*partitionTable
	kv          *mem.KV
}

func newMemPartitionStorage() PartitionStorage {
	s := &memStorage{
		id:          1000000,
		committed:   make(map[uint64]*partitionTable),
		uncommitted: make(map[uint64]*partitionTable),
		kv:          mem.NewKV(),
	}
	return s
}

func (s *memStorage) GetTableDef(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (*plan.TableDef, error) {
	s.RLock()
	defer s.RUnlock()
	m, ok := s.committed[tableID]
	if ok {
		return m.def, nil
	}

	m, ok = s.uncommitted[tableID]
	if ok {
		return m.def, nil
	}
	return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
}

func (s *memStorage) GetMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, bool, error) {
	s.RLock()
	defer s.RUnlock()

	m, ok := s.committed[tableID]
	if ok {
		return m.metadata, true, nil
	}

	if txnOp != nil {
		m, ok = s.uncommitted[tableID]
		if ok {
			return m.metadata, true, nil
		}
	}

	return partition.PartitionMetadata{}, false, nil
}

func (s *memStorage) Create(
	ctx context.Context,
	def *plan.TableDef,
	stmt *tree.CreateTable,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	s.Lock()
	defer s.Unlock()

	for _, p := range metadata.Partitions {
		p.PartitionID = s.nextTableID()
		if v, ok := s.uncommitted[def.TblId]; !ok {
			s.uncommitted[def.TblId] = &partitionTable{
				metadata:   metadata,
				def:        def,
				partitions: []partition.Partition{p},
			}
		} else {
			v.metadata = metadata
			v.partitions = append(v.partitions, p)
		}
	}

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		func(txn client.TxnEvent) {
			s.Lock()
			defer s.Unlock()

			v, ok := s.uncommitted[def.TblId]
			if txn.Committed() {
				if ok {
					s.committed[def.TblId] = v
				}
			}
			delete(s.uncommitted, def.TblId)
		},
	)
	return nil
}

func (s *memStorage) Delete(
	ctx context.Context,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	table := metadata.TableID
	if txnOp == nil {
		delete(s.committed, table)
		return nil
	}

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		func(txn client.TxnEvent) {
			s.Lock()
			defer s.Unlock()

			delete(s.uncommitted, table)
			if txn.Committed() {
				delete(s.committed, table)
			}
		},
	)
	return nil
}

func (s *memStorage) addUncommittedTable(
	def *plan.TableDef,
) {
	s.Lock()
	defer s.Unlock()

	s.uncommitted[def.TblId] = &partitionTable{
		def: def,
	}
}

func (s *memStorage) nextTableID() uint64 {
	return atomic.AddUint64(&s.id, 1)
}

type partitionTable struct {
	metadata   partition.PartitionMetadata
	def        *plan.TableDef
	partitions []partition.Partition
}

func TestGetPartitionTableCreateSQL(t *testing.T) {
	str := getPartitionTableCreateSQL(
		getCreateTableStatement(
			t,
			"create table t(a int) partition by hash(a) partitions 5",
		),
		partition.Partition{
			Name:               "p1",
			PartitionTableName: "test_p1",
		},
	)
	require.Equal(t, "create table `test_p1` (`a` int)", str)
}
