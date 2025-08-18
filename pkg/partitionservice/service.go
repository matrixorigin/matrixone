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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	DisabledService = NewService(Config{}, nil)
)

type Service struct {
	cfg   Config
	store PartitionStorage

	mu struct {
		sync.RWMutex
		tables map[uint64]metadataCache
	}
}

func NewService(
	cfg Config,
	store PartitionStorage,
) *Service {
	s := &Service{
		cfg:   cfg,
		store: store,
	}
	s.mu.tables = make(map[uint64]metadataCache)
	return s
}

func (s *Service) Create(
	ctx context.Context,
	tableID uint64,
	stmt *tree.CreateTable,
	txnOp client.TxnOperator,
) error {
	if !s.cfg.Enable {
		return nil
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	metadata, err := s.getMetadata(
		def,
		stmt.PartitionOption,
	)
	if err != nil {
		return err
	}

	return s.store.Create(
		ctx,
		def,
		stmt,
		metadata,
		txnOp,
	)
}

func (s *Service) Truncate(
	ctx context.Context,
	oldTableID uint64,
	newTableID uint64,
	txnOp client.TxnOperator,
) error {
	if !s.cfg.Enable {
		return nil
	}

	return s.store.Truncate(
		ctx,
		oldTableID,
		newTableID,
		txnOp,
	)
}

func (s *Service) Delete(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) error {
	if !s.cfg.Enable {
		return nil
	}

	metadata, ok, err := s.store.GetMetadata(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	if txnOp != nil {
		txnOp.AppendEventCallback(
			client.CommitEvent,
			func(te client.TxnEvent) {
				s.mu.Lock()
				delete(s.mu.tables, tableID)
				s.mu.Unlock()
			},
		)
	}

	err = s.store.Delete(
		ctx,
		metadata,
		txnOp,
	)
	if err == nil && txnOp == nil {
		s.mu.Lock()
		delete(s.mu.tables, tableID)
		s.mu.Unlock()
	}
	return err
}

func (s *Service) GetPartitionMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, error) {
	if !s.cfg.Enable {
		return partition.PartitionMetadata{}, nil
	}

	return s.readMetadata(ctx, tableID, txnOp)
}

func (s *Service) Enabled() bool {
	return s.cfg.Enable
}

func (s *Service) getMetadata(
	def *plan.TableDef,
	option *tree.PartitionOption,
) (partition.PartitionMetadata, error) {
	if option == nil || option.PartBy == nil {
		panic("BUG: partition option is nil")
	}
	if option.PartBy.IsSubPartition {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("sub-partition is not supported")
	}

	method := option.PartBy.PType
	switch method.(type) {
	case *tree.HashType:
		return s.getMetadataByHashType(
			option,
			def,
		)
	case *tree.KeyType:
		return s.getMetadataByKeyType(
			option,
			def,
		)
	case *tree.RangeType:
		return s.getMetadataByRangeType(
			option,
			def,
		)
	case *tree.ListType:
		return s.getMetadataByListType(
			option,
			def,
		)
	default:
		panic("BUG: unsupported partition method")
	}

}

func (s *Service) readMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, error) {
	s.mu.RLock()
	c, ok := s.mu.tables[tableID]
	s.mu.RUnlock()
	if ok {
		return c.metadata, nil
	}

	metadata, ok, err := s.store.GetMetadata(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}
	if !ok {
		return partition.PartitionMetadata{}, nil
	}

	s.mu.Lock()
	s.mu.tables[tableID] = newMetadataCache(metadata)
	s.mu.Unlock()
	return metadata, nil
}

func (s *Service) GetStorage() PartitionStorage {
	return s.store
}

func (s *Service) getManualPartitions(
	option *tree.PartitionOption,
	def *plan.TableDef,
	partitionDesc string,
	method partition.PartitionMethod,
	applyPartitionComment func(*tree.Partition) string,
) (partition.PartitionMetadata, error) {
	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       method,
		Description:  partitionDesc,
		Columns:      []string{partitionDesc},
	}

	for i, p := range option.Partitions {
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:               p.Name.String(),
				PartitionTableName: GetPartitionTableName(def.Name, p.Name.String()),
				Position:           uint32(i),
				ExprStr:            applyPartitionComment(p),
				Expr:               def.Partition.PartitionDefs[i].Def,
			},
		)
	}
	return metadata, nil
}

type metadataCache struct {
	metadata partition.PartitionMetadata
}

func newMetadataCache(
	metadata partition.PartitionMetadata,
) metadataCache {
	return metadataCache{
		metadata: metadata,
	}
}

func GetPartitionTableName(
	tableName string,
	partitionName string,
) string {
	return "%!%" + partitionName + "%!%" + tableName
}
