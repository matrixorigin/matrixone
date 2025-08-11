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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (s *Service) getMetadataByRangeType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.RangeType)

	var columns *tree.UnresolvedName
	desc := ""
	if method.Expr != nil {
		ctx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithEscapeSingleQuoteString(),
		)
		method.Expr.Format(ctx)
		desc = ctx.String()
	} else {
		if len(method.ColumnList) != 1 {
			return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in RANGE partition")
		}

		columns = method.ColumnList[0]
		ctx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithEscapeSingleQuoteString(),
		)
		columns.Format(ctx)
		desc = ctx.String()
	}

	return s.getManualPartitions(
		option,
		def,
		desc,
		partition.PartitionMethod_Range,
		func(p *tree.Partition) string {
			ctx := tree.NewFmtCtx(
				dialect.MYSQL,
				tree.WithQuoteIdentifier(),
				tree.WithEscapeSingleQuoteString(),
			)
			p.Values.Format(ctx)
			return ctx.String()
		},
	)
}
