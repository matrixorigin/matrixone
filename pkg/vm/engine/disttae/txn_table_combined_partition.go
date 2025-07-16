// Copyright 2021-2025 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/partitionprune"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getPartitionPrunePKFunc(
	proc *process.Process,
	metadata partition.PartitionMetadata,
	db *txnDatabase,
) prunePKFunc {
	return func(
		bat *batch.Batch,
		partitionIndex int32,
	) ([]engine.Relation, error) {
		res, err := partitionprune.Prune(proc, bat, metadata, partitionIndex)
		if err != nil {
			return nil, err
		}
		defer res.Close()

		var rel engine.Relation
		relations := make([]engine.Relation, 0, len(metadata.Partitions))
		res.Iter(
			func(p partition.Partition, bat *batch.Batch) bool {
				rel, err = db.relation(
					proc.Ctx,
					p.PartitionTableName,
					proc,
				)
				if err != nil {
					return false
				}
				relations = append(relations, rel)
				return true
			},
		)
		return relations, err
	}
}

func getPartitionTableFunc(
	proc *process.Process,
	metadata partition.PartitionMetadata,
	db *txnDatabase,
) tablesFunc {
	relations := make([]engine.Relation, 0, len(metadata.Partitions))
	return func() ([]engine.Relation, error) {
		if len(relations) > 0 {
			return relations, nil
		}
		for _, p := range metadata.Partitions {
			rel, err := db.relation(
				proc.Ctx,
				p.PartitionTableName,
				proc,
			)
			if err != nil {
				relations = relations[:0]
				return nil, err
			}
			relations = append(relations, rel)
		}
		return relations, nil
	}
}

func getPruneTablesFunc(
	proc *process.Process,
	metadata partition.PartitionMetadata,
	db *txnDatabase,
) pruneFunc {
	return func(
		ctx context.Context,
		param engine.RangesParam,
	) ([]engine.Relation, error) {
		targets, err := partitionprune.Filter(
			proc,
			param.BlockFilters,
			metadata,
		)
		if err != nil {
			return nil, err
		}

		relations := make([]engine.Relation, 0, len(targets))
		for _, idx := range targets {
			rel, err := db.relation(
				ctx,
				metadata.Partitions[idx].PartitionTableName,
				proc,
			)
			if err != nil {
				return nil, err
			}
			relations = append(relations, rel)
		}
		return relations, nil
	}
}
