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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/partitionprune"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (t *combinedTxnTable) relationsForPKCheck(
	ctx context.Context,
	bat *batch.Batch,
	pkIndex, partitionIndex int32,
) ([]engine.Relation, error) {
	def := t.primary.GetTableDef(ctx)
	if partitionIndex >= 0 && def.Partition != nil {
		if _, ok := partitionSingleColumn(def); !ok {
			return t.tablesFunc()
		}
	}
	if partitionIndex < 0 {
		if def.Partition != nil {
			// A lock input is a projection, not necessarily a base-table row.
			// In particular, ODKU can resolve an incoming PK to a different
			// existing row through a UNIQUE key. Never prune by the incoming row.
			if !partitionUsesOnlyPrimaryKey(def) {
				return t.tablesFunc()
			}
			partitionIndex = pkIndex
			if bat.Vecs[0].GetType().Oid == types.T_Rowid {
				// Prune's explicit index excludes a leading physical RowID.
				partitionIndex--
			}
		}
	}
	return t.prunePKFunc(bat, partitionIndex)
}

func partitionUsesOnlyPrimaryKey(def *plan.TableDef) bool {
	if def.Pkey == nil || len(def.Pkey.Names) != 1 || def.Pkey.PkeyColName != def.Pkey.Names[0] {
		return false
	}
	pos, ok := def.Name2ColIndex[def.Pkey.PkeyColName]
	source, single := partitionSingleColumn(def)
	return ok && single && source == pos
}

// A lock projection supplies one partition value, so every definition must
// depend on the same schema column before that value can select relations.
func partitionSingleColumn(def *plan.TableDef) (int32, bool) {
	if def.Partition == nil || len(def.Partition.PartitionDefs) == 0 {
		return -1, false
	}
	source := int32(-1)
	var visit func(*plan.Expr) bool
	visit = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		switch e := expr.Expr.(type) {
		case *plan.Expr_Col:
			if source == -1 {
				source = e.Col.ColPos
			}
			return e.Col.ColPos >= 0 && e.Col.ColPos == source
		case *plan.Expr_F:
			for _, arg := range e.F.Args {
				if !visit(arg) {
					return false
				}
			}
			return true
		case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
			return true
		default:
			return false
		}
	}
	for _, partition := range def.Partition.PartitionDefs {
		if !visit(partition.Def) {
			return -1, false
		}
	}
	return source, source >= 0
}

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
