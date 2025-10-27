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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	PartitionTableMetadataSQL = fmt.Sprintf(`create table %s.%s(
		table_id 		           bigint        unsigned primary key not null,  
		table_name                 varchar(500)                       not null,
		database_name			   varchar(500)                       not null,
		partition_method           varchar(13)                        not null,  
		partition_description      text                               not null,
		partition_count            int	         unsigned
	)`, catalog.MO_CATALOG, catalog.MOPartitionMetadata)

	PartitionTablesSQL = fmt.Sprintf(`create table %s.%s(
		partition_id               bigint        unsigned not null,
		partition_table_name       varchar(200)  not null,
		primary_table_id 		   bigint        unsigned not null, 
		partition_name             varchar(50)   not null,
		partition_ordinal_position int	         unsigned not null,
		partition_expression_str   varchar(2048) not null,
    	partition_expression       varchar(2048) not null
	)`, catalog.MO_CATALOG, catalog.MOPartitionTables)

	InitSQLs = []string{
		PartitionTablesSQL,
		PartitionTableMetadataSQL,
	}
)

// PartitionService is used to maintaining the metadata of the partition table.
type PartitionService interface {
	Create(
		ctx context.Context,
		tableID uint64,
		stmt *tree.CreateTable,
		txnOp client.TxnOperator,
	) error

	Redefine(
		ctx context.Context,
		tableID uint64,
		stmt *tree.PartitionOption,
		txnOp client.TxnOperator,
	) error

	Rename(
		ctx context.Context,
		tableID uint64,
		oldName, newName string,
		txnOp client.TxnOperator,
	) error

	Delete(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) error

	AddPartitions(
		ctx context.Context,
		tableID uint64,
		partitions []*tree.Partition,
		partitionDefs []*plan.PartitionDef,
		txnOp client.TxnOperator,
	) error

	DropPartitions(
		ctx context.Context,
		tableID uint64,
		partitions []string,
		txnOp client.TxnOperator,
	) error

	TruncatePartitions(
		ctx context.Context,
		tableID uint64,
		partitions []string,
		txnOp client.TxnOperator,
	) error

	GetPartitionMetadata(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) (partition.PartitionMetadata, error)

	GetStorage() PartitionStorage

	Enabled() bool
}

type PartitionStorage interface {
	GetMetadata(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) (partition.PartitionMetadata, bool, error)

	Create(
		ctx context.Context,
		def *plan.TableDef,
		stmt *tree.CreateTable,
		metadata partition.PartitionMetadata,
		txnOp client.TxnOperator,
	) error

	Redefine(
		ctx context.Context,
		def *plan.TableDef,
		options *tree.PartitionOption,
		metadata partition.PartitionMetadata,
		txnOp client.TxnOperator,
	) error

	Rename(
		ctx context.Context,
		def *plan.TableDef,
		oldName, newName string,
		metadata partition.PartitionMetadata,
		txnOp client.TxnOperator,
	) error

	AddPartitions(
		ctx context.Context,
		def *plan.TableDef,
		metadata partition.PartitionMetadata,
		partitions []partition.Partition,
		txnOp client.TxnOperator,
	) error

	DropPartitions(
		ctx context.Context,
		def *plan.TableDef,
		metadata partition.PartitionMetadata,
		partitions []string,
		txnOp client.TxnOperator,
	) error

	TruncatePartitions(
		ctx context.Context,
		def *plan.TableDef,
		metadata partition.PartitionMetadata,
		partitions []string,
		txnOp client.TxnOperator,
	) error

	Delete(
		ctx context.Context,
		metadata partition.PartitionMetadata,
		txnOp client.TxnOperator,
	) error

	GetTableDef(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) (*plan.TableDef, error)
}

func GetService(
	sid string,
) PartitionService {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.PartitionService)
	if !ok {
		return nil
	}
	return v.(PartitionService)
}

type PruneResult struct {
	batches    []*batch.Batch
	partitions []partition.Partition
}

func NewPruneResult(bats []*batch.Batch, partitions []partition.Partition) PruneResult {
	var pr PruneResult
	for i := range bats {
		if bats[i].RowCount() != 0 {
			pr.batches = append(pr.batches, bats[i])
			pr.partitions = append(pr.partitions, partitions[i])
		}
	}
	return pr
}

func (res PruneResult) Iter(fn func(partition partition.Partition, bat *batch.Batch) bool) {
	for i, p := range res.partitions {
		if !fn(p, res.batches[i]) {
			break
		}
	}
}

func (res PruneResult) Close() {

}

func (res PruneResult) Empty() bool {
	return len(res.partitions) == 0
}
