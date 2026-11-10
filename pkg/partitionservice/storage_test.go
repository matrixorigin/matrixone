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
		client.NewTxnEventCallback(
			func(ctx context.Context, txnOp client.TxnOperator, txn client.TxnEvent, cbdata any) error {
				s.Lock()
				defer s.Unlock()

				v, ok := s.uncommitted[def.TblId]
				if txn.Committed() {
					if ok {
						s.committed[def.TblId] = v
					}
				}
				delete(s.uncommitted, def.TblId)
				return nil
			}),
	)
	return nil
}

func (s *memStorage) Truncate(
	ctx context.Context,
	oldTableID uint64,
	newTableID uint64,
	txnOp client.TxnOperator,
) error {
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
		client.NewTxnEventCallback(
			func(ctx context.Context, txnOp client.TxnOperator, txn client.TxnEvent, v any) error {
				s.Lock()
				defer s.Unlock()

				delete(s.uncommitted, table)
				if txn.Committed() {
					delete(s.committed, table)
				}
				return nil
			}),
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

func (s *memStorage) AddPartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []partition.Partition,
	txnOp client.TxnOperator,
) error {
	return nil
}

func (s *memStorage) DropPartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	return nil
}

func (s *memStorage) TruncatePartitions(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	return nil
}

func (s *memStorage) Rename(
	ctx context.Context,
	def *plan.TableDef,
	oldName, newName string,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	return nil
}

func (s *memStorage) Redefine(
	ctx context.Context,
	def *plan.TableDef,
	options *tree.PartitionOption,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	return nil
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

func TestRowIDPartitionMetadataShiftsEveryColumn(t *testing.T) {
	col := func(pos int32) *plan.Expr { return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}} }
	for _, reverse := range []bool{false, true} {
		direct := col(0)
		nested := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{col(0), col(2)}}}}
		args := []*plan.Expr{direct, nested}
		if reverse {
			args[0], args[1] = args[1], args[0]
		}
		expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: args}}}
		data, err := expr.Marshal()
		require.NoError(t, err)
		shifted := getExprWithRowID(data)
		require.Equal(t, int32(0), direct.GetCol().ColPos, "source metadata must remain immutable")
		actualDirect, actualNested := shifted.GetF().Args[0], shifted.GetF().Args[1]
		if reverse {
			actualDirect, actualNested = actualNested, actualDirect
		}
		require.Equal(t, int32(1), actualDirect.GetCol().ColPos)
		require.Equal(t, int32(1), actualNested.GetF().Args[0].GetCol().ColPos)
		require.Equal(t, int32(3), actualNested.GetF().Args[1].GetCol().ColPos)
	}
	root := col(2)
	data, err := root.Marshal()
	require.NoError(t, err)
	require.Equal(t, int32(3), getExprWithRowID(data).GetCol().ColPos)
}
