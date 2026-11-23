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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (s *Service) getMetadataByKeyType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.KeyType)

	if len(method.ColumnList) == 0 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("none-column is not supported in KEY partition")
	}
	// KEY partitioning hashes the stored bytes; vecf8/vecf4 cells equal by decoded value can
	// differ in bytes
	for _, name := range method.ColumnList {
		for _, col := range def.Cols {
			if t := types.T(col.Typ.Id); t.IsBlockScaledArray() && strings.EqualFold(col.Name, name.ColName()) {
				return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtxf(
					"%s column '%s' cannot be a partition key", t, name.ColNameOrigin())
			}
		}
	}

	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	method.Format(ctx)

	pm := partition.PartitionMethod_Key
	if method.Linear {
		pm = partition.PartitionMethod_LinearKey
	}

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       pm,
		Description:  ctx.String(),
		Columns:      []string{ctx.String()},
	}

	for i := uint64(0); i < option.PartBy.Num; i++ {
		name := fmt.Sprintf("p%d", i)
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:               name,
				PartitionTableName: GetPartitionTableName(def.Name, name),
				Position:           uint32(i),
				Expr:               def.Partition.PartitionDefs[i].Def,
			},
		)
	}
	return metadata, nil
}
