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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

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
		tree.WithEscapeSingleQuoteString(),
	)
	method.Expr.Format(ctx)

	pm := partition.PartitionMethod_Hash
	if method.Linear {
		pm = partition.PartitionMethod_LinearHash
	}

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       pm,
		Description:  ctx.String(),
		Columns:      validColumns,
	}

	for i := uint64(0); i < option.PartBy.Num; i++ {
		name := fmt.Sprintf("p%d", i)
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:               name,
				PartitionTableName: fmt.Sprintf("%s_%s", def.Name, name),
				Position:           uint32(i),
			},
		)
	}
	return metadata, nil
}
