// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type repeatedSafetyTarget struct {
	table   *planpb.TableDef
	changed map[uint64]struct{}
}

// repeatedUpdateSafetyBoundary rejects updates that the final-image writer
// cannot safely maintain. Payload-only updates remain eligible.
func repeatedUpdateSafetyBoundary(builder *QueryBuilder, bindCtx *BindContext, plans []*dmlPlanCtx) error {
	checks, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil {
		return err
	}
	targets := make(map[uint64]*repeatedSafetyTarget, len(plans))
	ordered := make([]*repeatedSafetyTarget, 0, len(plans))
	for _, up := range plans {
		table := up.tableDef
		if len(table.Fkeys) > 0 || len(table.RefChildTbls) > 0 {
			// Cached plans must be rebound when foreign_key_checks changes.
			builder.qry.HasForeignKeyAction = true
		}
		target := targets[table.TblId]
		if target == nil {
			target = &repeatedSafetyTarget{table: table, changed: make(map[uint64]struct{})}
			targets[table.TblId] = target
			ordered = append(ordered, target)
		}
		names := make([]string, 0, len(up.updateColPosMap))
		for name := range up.updateColPosMap {
			names = append(names, name)
			pos, ok := table.Name2ColIndex[name]
			if !ok || pos < 0 || int(pos) >= len(table.Cols) {
				return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE foreign-key column")
			}
			target.changed[table.Cols[pos].ColId] = struct{}{}
		}
		generated, err := repeatedGeneratedColumns(table, names)
		if err != nil {
			return err
		}
		for _, pos := range generated {
			target.changed[table.Cols[pos].ColId] = struct{}{}
		}
	}
	for _, target := range ordered {
		if target.table.Partition != nil {
			return moerr.NewUnsupportedDML(builder.GetContext(), "repeated-target UPDATE writing a partitioned table")
		}
		for _, col := range target.table.Cols {
			if _, changed := target.changed[col.ColId]; changed && col.Typ.AutoIncr {
				return moerr.NewUnsupportedDML(builder.GetContext(), "repeated-target UPDATE changing an auto-increment column")
			}
		}
		if target.table.Pkey != nil {
			for _, name := range target.table.Pkey.Names {
				pos, ok := target.table.Name2ColIndex[name]
				if !ok || pos < 0 || int(pos) >= len(target.table.Cols) {
					return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE primary-key column")
				}
				if _, changed := target.changed[target.table.Cols[pos].ColId]; !changed {
					continue
				}
				for _, index := range target.table.Indexes {
					if isSyncMaintainedIrregularIndex(index) {
						return moerr.NewUnsupportedDML(builder.GetContext(), "update primary key on a table with a synchronous full-text/vector index")
					}
				}
			}
		}
		if !checks {
			continue
		}
		for _, fk := range target.table.Fkeys {
			for _, col := range fk.Cols {
				if _, changed := target.changed[col]; changed {
					return moerr.NewUnsupportedDML(builder.GetContext(), "repeated-target UPDATE changing foreign-key columns")
				}
			}
		}
		seen := make(map[uint64]struct{}, len(target.table.RefChildTbls))
		for _, childID := range target.table.RefChildTbls {
			if childID == 0 {
				childID = target.table.TblId
			}
			if _, exists := seen[childID]; exists {
				continue
			}
			seen[childID] = struct{}{}
			_, child, err := builder.compCtx.ResolveById(childID, bindCtx.snapshot)
			if err != nil {
				return err
			}
			if child == nil {
				return moerr.NewInternalErrorf(builder.GetContext(), "foreign-key child table %d not found", childID)
			}
			for _, fk := range child.Fkeys {
				if fk.ForeignTbl != target.table.TblId && !(fk.ForeignTbl == 0 && child.TblId == target.table.TblId) {
					continue
				}
				for _, col := range fk.ForeignCols {
					if _, changed := target.changed[col]; changed {
						return moerr.NewUnsupportedDML(builder.GetContext(), "repeated-target UPDATE changing a referenced foreign-key column")
					}
				}
			}
		}
	}
	return nil
}
