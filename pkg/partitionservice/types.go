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
		partition_expression       varchar(2048)                      not null,
		partition_description      text                               not null,
		partition_count            int	         unsigned
	)`, catalog.MO_CATALOG, catalog.MOPartitionMetadata)

	PartitionTablesSQL = fmt.Sprintf(`create table %s.%s(
		partition_id               bigint        unsigned not null,
		partition_table_name       varchar(200)  not null,
		primary_table_id 		   bigint        unsigned not null, 
		partition_name             varchar(50)   not null,
		partition_ordinal_position int	         unsigned not null,
		partition_comment          text
	)`, catalog.MO_CATALOG, catalog.MOPartitionTables)

	InitSQLs = []string{
		PartitionTablesSQL,
		PartitionTableMetadataSQL,
	}
)

// PartitionService is used to maintaining the metadata of the partition table.
type PartitionService interface {
	Is(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) (bool, partition.PartitionMetadata, error)

	// Create creates metadata of the partition table.
	Create(
		ctx context.Context,
		tableID uint64,
		stmt *tree.CreateTable,
		txnOp client.TxnOperator,
	) error

	Delete(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) error

	GetStorage() PartitionStorage
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
