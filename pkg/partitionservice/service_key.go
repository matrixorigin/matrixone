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
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (s *Service) getMetadataByKeyType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.KeyType)
	if option.PartBy.Num <= 0 {
		return partition.PartitionMetadata{}, moerr.NewInvalidInputNoCtx("partition number is invalid")
	}

	if len(method.ColumnList) == 0 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("none-column is not supported in KEY partition")
	}

	if len(method.ColumnList) > 1 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in KEY partition")
	}

	validColumns, err := validColumns(
		method.ColumnList[0],
		def,
		func(t plan.Type) bool {
			return true
		},
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}

	pm := partition.PartitionMethod_Hash
	if method.Linear {
		pm = partition.PartitionMethod_LinearHash
	}

	metadata := partition.PartitionMetadata{
		TableID:      def.TblId,
		TableName:    def.Name,
		DatabaseName: def.DbName,
		Method:       pm,
		Description:  fmt.Sprintf("`%s`", validColumns[0]),
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
				Expr:               def.Partition.PartitionDefs[i].Def,
			},
		)
	}
	return metadata, nil
}
