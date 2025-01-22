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

	var columns *tree.UnresolvedName
	var ok bool
	var validTypeFunc func(t plan.Type) bool
	desc := ""
	if method.Expr != nil {
		columns, ok = method.Expr.(*tree.UnresolvedName)
		if !ok {
			return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("column expression is not supported")
		}
		if columns.NumParts != 1 {
			return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in RANGE partition")
		}

		validTypeFunc = func(t plan.Type) bool {
			return types.T(t.Id).IsInteger()
		}

		ctx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithEscapeSingleQuoteString(),
		)
		method.Expr.Format(ctx)
		desc = ctx.String()
	} else {
		if len(method.ColumnList) != 1 {
			return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in RANGE partition")
		}

		columns = method.ColumnList[0]
		validTypeFunc = func(t plan.Type) bool {
			return types.T(t.Id) == types.T_date || types.T(t.Id) == types.T_datetime
		}
	}

	validColumns, err := validColumns(
		columns,
		def,
		validTypeFunc,
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       partition.PartitionMethod_Range,
		// TODO: ???
		Expression:  "",
		Description: desc,
		Columns:     validColumns,
	}

	for i, p := range option.Partitions {
		ctx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithEscapeSingleQuoteString(),
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
