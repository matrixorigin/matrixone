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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	DisabledService = NewService(Config{}, nil)
)

type Service struct {
	cfg   Config
	store PartitionStorage
}

func NewService(
	cfg Config,
	store PartitionStorage,
) *Service {
	s := &Service{
		cfg:   cfg,
		store: store,
	}
	return s
}

func (s *Service) Create(
	ctx context.Context,
	tableID uint64,
	stmt *tree.CreateTable,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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

func (s *Service) Redefine(
	ctx context.Context,
	tableID uint64,
	stmt *tree.PartitionOption,
	txnOp client.TxnOperator,
) error {
	metadata, ok, err := s.store.GetMetadata(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}
	if !ok {
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %d is not partitioned", tableID))
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	return s.store.Redefine(
		ctx,
		def,
		stmt,
		metadata,
		txnOp,
	)
}

func (s *Service) Rename(
	ctx context.Context,
	tableID uint64,
	oldName, newName string,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %d is not partitioned", tableID))
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	return s.store.Rename(
		ctx,
		def,
		oldName,
		newName,
		metadata,
		txnOp,
	)
}

func (s *Service) AddPartitions(
	ctx context.Context,
	tableID uint64,
	partitions []*tree.Partition,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %d is not partitioned", tableID))
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	switch metadata.Method {
	case partition.PartitionMethod_Hash,
		partition.PartitionMethod_Key,
		partition.PartitionMethod_LinearHash,
		partition.PartitionMethod_LinearKey:
		return moerr.NewNotSupportedNoCtx("add partition is not supported for hash/key partitioned table")
	case partition.PartitionMethod_Range:
		// TODO: check overlapping range

	case partition.PartitionMethod_List:
		// TODO: check overlapping list values
	default:
		panic("BUG: unsupported partition method")
	}

	values := make([]partition.Partition, 0, len(partitions))
	n := len(metadata.Partitions)
	for i, p := range partitions {
		values = append(values,
			partition.Partition{
				Name:               p.Name.String(),
				PartitionTableName: GetPartitionTableName(def.Name, p.Name.String()),
				Position:           uint32(i + n),
				ExprStr:            getExpr(p),
				Expr:               newTestValuesInExpr2(p.Name.String()),
			},
		)
	}

	return s.store.AddPartitions(
		ctx,
		def,
		metadata,
		values,
		txnOp,
	)
}

func (s *Service) DropPartitions(
	ctx context.Context,
	tableID uint64,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %d is not partitioned", tableID))
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	switch metadata.Method {
	case partition.PartitionMethod_Hash,
		partition.PartitionMethod_Key,
		partition.PartitionMethod_LinearHash,
		partition.PartitionMethod_LinearKey:
		return moerr.NewNotSupportedNoCtx("drop partition is not supported for hash/key partitioned table")
	case partition.PartitionMethod_Range:
	case partition.PartitionMethod_List:
	default:
		panic("BUG: unsupported partition method")
	}

	return s.store.DropPartitions(
		ctx,
		def,
		metadata,
		partitions,
		txnOp,
	)
}

func (s *Service) TruncatePartitions(
	ctx context.Context,
	tableID uint64,
	partitions []string,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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
		return moerr.NewInternalError(ctx, fmt.Sprintf("table %d is not partitioned", tableID))
	}

	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	if len(partitions) == 0 {
		for _, p := range metadata.Partitions {
			partitions = append(partitions, p.Name)
		}
	}

	return s.store.TruncatePartitions(
		ctx,
		def,
		metadata,
		partitions,
		txnOp,
	)
}

func (s *Service) Delete(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) error {
	if s.cfg.Disable {
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

func (s *Service) GetPartitionMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, error) {
	if s.cfg.Disable {
		return partition.PartitionMetadata{}, nil
	}

	return s.readMetadata(ctx, tableID, txnOp)
}

func (s *Service) Enabled() bool {
	return !s.cfg.Disable
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
	// TODO: use cache
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

func GetPartitionTableName(
	tableName string,
	partitionName string,
) string {
	return "%!%" + partitionName + "%!%" + tableName
}

func getExpr(p *tree.Partition) string {
	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	p.Values.Format(ctx)
	return ctx.String()
}

func newTestValuesInExpr2(col string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: 10},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "in",
					Obj:     506806140934,
				},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: 22},
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: col},
						},
					},
					{
						Typ: plan.Type{Id: 202},
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: []*plan.Expr{
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{
													Obj:     90194313216,
													ObjName: "cast",
												},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 1},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 23},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
										},
									},
									{
										Typ: plan.Type{Id: 22},
										Expr: &plan.Expr_F{
											F: &plan.Function{
												Func: &plan.ObjectRef{ObjName: "cast", Obj: 90194313216},
												Args: []*plan.Expr{
													{
														Typ: plan.Type{Id: 23},
														Expr: &plan.Expr_Lit{
															Lit: &plan.Literal{
																Value: &plan.Literal_I64Val{I64Val: 2},
															},
														},
													},
													{
														Typ:  plan.Type{Id: 22},
														Expr: &plan.Expr_T{T: &plan.TargetType{}},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
