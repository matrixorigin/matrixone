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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

	// a range bound compared with a bf16/float16/float8/float4 column rounds to the column
	// type, as a literal does, so a value rounding onto a bound has no partition
	names := method.ColumnList
	expr := method.Expr
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = paren.Expr
	}
	if name, ok := expr.(*tree.UnresolvedName); ok {
		names = append(names[:len(names):len(names)], name)
	}
	for _, name := range names {
		for _, col := range def.Cols {
			if t := types.T(col.Typ.Id); t.IsLowPrecisionFloat() && strings.EqualFold(col.Name, name.ColName()) {
				return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtxf(
					"%s column '%s' cannot be a RANGE partition column", t, name.ColNameOrigin())
			}
		}
	}

	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)

	method.Format(ctx)
	desc := ctx.String()

	return s.getManualPartitions(
		option,
		def,
		desc,
		partition.PartitionMethod_Range,
		getExpr,
	)
}
