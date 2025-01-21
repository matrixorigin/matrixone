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

func (s *service) getMetadataByRangeType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.RangeType)

	columns, ok := method.Expr.(*tree.UnresolvedName)
	if !ok {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("column expression is not supported")
	}
	if columns.NumParts != 1 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in RANGE partition")
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
		tree.WithSingleQuoteString(),
	)
	method.Expr.Format(ctx)

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       partition.PartitionMethod_Range,
		// TODO: ???
		Expression:  "",
		Description: ctx.String(),
		Columns:     validColumns,
	}

	for i, p := range option.Partitions {
		ctx = tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithSingleQuoteString(),
		)
		p.Values.Format(ctx)

		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:               p.Name.String(),
				PartitionTableName: fmt.Sprintf("%s_%s", def.Name, p.Name.String()),
				Position:           uint32(i),
				Comment:            ctx.String(),
			},
		)
	}
	return metadata, nil
}
