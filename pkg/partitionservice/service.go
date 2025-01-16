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
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type service struct {
	cfg   Config
	store PartitionStorage

	mu struct {
		sync.RWMutex
		tables map[uint64]partition.PartitionMetadata
	}
}

func NewService(
	cfg Config,
	store PartitionStorage,
) PartitionService {
	s := &service{
		cfg:   cfg,
		store: store,
	}
	s.mu.tables = make(map[uint64]partition.PartitionMetadata)
	return s
}

func (s *service) Create(
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

func (s *service) Delete(
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

	return s.store.Delete(
		ctx,
		metadata,
		txnOp,
	)
}

func (s *service) Is(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (bool, partition.PartitionMetadata, error) {
	if !s.cfg.Enable {
		return false, partition.PartitionMetadata{}, nil
	}

	metadata, err := s.readMetadata(ctx, tableID, txnOp)
	if err != nil {
		return false, partition.PartitionMetadata{}, err
	}
	return !metadata.IsEmpty(), metadata, nil
}

func (s *service) getMetadata(
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
	default:
		panic("BUG: unsupported partition method")
	}

}

func (s *service) getMetadataByHashType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.HashType)
	if option.PartBy.Num <= 0 {
		return partition.PartitionMetadata{}, moerr.NewInvalidInputNoCtx("partition number is invalid")
	}

	columns, ok := method.Expr.(*tree.UnresolvedName)
	if !ok {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("column expression is not supported")
	}
	if columns.NumParts != 1 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in HASH partition")
	}
	validColumns, err := validColumns(
		columns,
		def,
		func(t plan.Type) bool {
			return types.T(t.Id).IsInteger()
		},
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}

	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteString(true),
	)
	method.Expr.Format(ctx)

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       partition.PartitionMethod_Hash,
		// TODO: ???
		Expression:  "",
		Description: ctx.String(),
		Columns:     validColumns,
	}

	for i := uint64(0); i < option.PartBy.Num; i++ {
		name := fmt.Sprintf("p%d", i)
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:               name,
				PartitionTableName: fmt.Sprintf("%s_%s", def.Name, name),
				Position:           uint32(i),
				Comment:            "",
			},
		)
	}
	return metadata, nil
}

func (s *service) readMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, error) {
	s.mu.RLock()
	metadata, ok := s.mu.tables[tableID]
	s.mu.RUnlock()
	if ok {
		return metadata, nil
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
	s.mu.tables[tableID] = metadata
	s.mu.Unlock()
	return metadata, nil
}

func (s *service) GetStorage() PartitionStorage {
	return s.store
}

func validColumns(
	columns *tree.UnresolvedName,
	tableDefine *plan.TableDef,
	validType func(plan.Type) bool,
) ([]string, error) {
	validColumns := make([]string, 0, columns.NumParts)
	for i := 0; i < columns.NumParts; i++ {
		v := columns.CStrParts[i]
		col := v.Compare()
		has := false
		for _, c := range tableDefine.GetCols() {
			if !strings.EqualFold(c.Name, col) {
				continue
			}

			has = true
			if !validType(c.Typ) {
				return nil, moerr.NewNotSupportedNoCtx("column type is not supported in hash partition")
			}
			break
		}
		if !has {
			return nil, moerr.NewErrWrongColumnName(moerr.Context(), v.Origin())
		}
		validColumns = append(validColumns, col)
	}
	return validColumns, nil
}
