// Copyright 2022 Matrix Origin
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

package plan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// AddTablePartitions will add a new partition to the table.
func AddTablePartitions(ctx CompilerContext, alterTable *plan.AlterTable, spec *tree.AlterPartitionAddPartitionClause) (*plan.AlterTableAddPartition, error) {
	partInfo := alterTable.TableDef.GetPartition()
	if partInfo == nil {
		return nil, moerr.NewErrPartitionMgmtOnNonpartitioned(ctx.GetContext())
	}

	switch partInfo.Type {
	case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY, plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
		// Adding partitions to hash/key is actually a reorganization of partition operations,
		// not just changing metadata! Error not supported
		return nil, moerr.NewNotSupported(ctx.GetContext(), "ADD PARTITION to HASH/KEY partitioned table")
	case plan.PartitionType_RANGE, plan.PartitionType_RANGE_COLUMNS:
		if len(spec.Partitions) == 0 {
			return nil, moerr.NewPartitionsMustBeDefined(ctx.GetContext(), "RANGE")
		}
	case plan.PartitionType_LIST, plan.PartitionType_LIST_COLUMNS:
		if len(spec.Partitions) == 0 {
			return nil, moerr.NewPartitionsMustBeDefined(ctx.GetContext(), "LIST")
		}
	default:
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Unsupported Add Partition")
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindContext := NewBindContext(builder, nil)
	nodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       nil,
		ObjRef:      nil,
		TableDef:    alterTable.TableDef,
		BindingTags: []int32{builder.genNewTag()},
	}, bindContext)

	err := builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
	if err != nil {
		return nil, err
	}
	partitionBinder := NewPartitionBinder(builder, bindContext)
	alterAddPartition, err := buildAddPartitionClause(ctx.GetContext(), partitionBinder, spec, alterTable.TableDef)
	if err != nil {
		return nil, err
	}
	return alterAddPartition, err
}

// buildAddPartitionClause build alter table add partition clause info and semantic check.
func buildAddPartitionClause(ctx context.Context, partitionBinder *PartitionBinder, stmt *tree.AlterPartitionAddPartitionClause, tableDef *TableDef) (*plan.AlterTableAddPartition, error) {
	//var builder partitionBuilder
	switch tableDef.Partition.Type {
	case plan.PartitionType_RANGE, plan.PartitionType_RANGE_COLUMNS:
		//builder := &rangePartitionBuilder{}
		builder2 := &rangePartitionBuilder{}
		err := builder2.buildAddPartition(ctx, partitionBinder, stmt, tableDef)
		if err != nil {
			return nil, err
		}
		alterAddPartition := &plan.AlterTableAddPartition{
			Definitions:     builder2.addPartitions,
			PartitionTables: builder2.addPartitionSubTableDefs,
			PartitionDef:    builder2.newPartitionDef,
		}
		return alterAddPartition, nil
	case plan.PartitionType_LIST, plan.PartitionType_LIST_COLUMNS:
		//builder = &listPartitionBuilder{}
		builder2 := &listPartitionBuilder{}
		err := builder2.buildAddPartition(ctx, partitionBinder, stmt, tableDef)
		if err != nil {
			return nil, err
		}
		alterAddPartition := &plan.AlterTableAddPartition{
			Definitions:     builder2.addPartitions,
			PartitionTables: builder2.addPartitionSubTableDefs,
			PartitionDef:    builder2.newPartitionDef,
		}
		return alterAddPartition, nil
	}
	return nil, nil
}
