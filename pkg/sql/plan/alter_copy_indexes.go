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
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// alterCopySourceColumn proves identity against the actual projection, including
// intermediate DROP/ADD and chained renames. Column ID zero is a valid identity.
func alterCopySourceColumn(old, target *TableDef, ctx *AlterTableContext, name string, preserved map[string]bool) *ColDef {
	source, ok := ctx.alterColMap[name]
	if !ok || source.sexprType != exprColumnName {
		return nil
	}
	src, dst := FindColumn(old.Cols, source.sexprStr), FindColumn(target.Cols, name)
	if src == nil || dst == nil || src.Hidden || dst.Hidden || src.ColId == UnKnownColId || src.ColId != dst.ColId || src.Seqnum != dst.Seqnum {
		return nil
	}
	mapped := ctx.changColDefMap[src.ColId]
	if mapped == nil || !strings.EqualFold(mapped.Name, dst.Name) {
		return nil
	}
	for _, col := range old.Cols {
		if col != src && col.ColId == src.ColId {
			return nil
		}
	}
	if src.GeneratedCol != nil && preserved[dst.Name] {
		return src
	}
	if !AlterCopyKeyColumnValueUnchanged(src, dst, true) {
		return nil
	}
	return src
}

func alterCopyPrimaryKeyUnchanged(old, target *TableDef, ctx *AlterTableContext, preserved map[string]bool) bool {
	a, b := old.Pkey, target.Pkey
	if a == nil || b == nil {
		return false
	}
	if catalog.IsFakePkName(a.PkeyColName) || catalog.IsFakePkName(b.PkeyColName) {
		return a.PkeyColName == b.PkeyColName
	}
	oldParts, newParts := AlterCopyPrimaryKeyParts(a), AlterCopyPrimaryKeyParts(b)
	if len(oldParts) == 0 || len(oldParts) != len(newParts) {
		return false
	}
	for i, name := range newParts {
		src := alterCopySourceColumn(old, target, ctx, name, preserved)
		if src == nil || !strings.EqualFold(src.Name, oldParts[i]) {
			return false
		}
	}
	return true
}

// alterCopyCloneIndexes has one decision per logical index, shared by INSERT
// maintenance, hidden-table cloning and post-copy plugin rebuilding. All physical
// definitions must survive unchanged; a matching name alone is insufficient.
func alterCopyCloneIndexes(old, target *TableDef, ctx *AlterTableContext, preserved map[string]bool, locatorUnchanged bool) (map[string]bool, error) {
	clone := make(map[string]bool)
	if !locatorUnchanged {
		return clone, nil
	}
	byName := make(map[string][]*plan.IndexDef)
	for _, idx := range target.Indexes {
		byName[idx.IndexName] = append(byName[idx.IndexName], idx)
	}
	oldByName := make(map[string][]*plan.IndexDef)
	for _, idx := range old.Indexes {
		oldByName[idx.IndexName] = append(oldByName[idx.IndexName], idx)
	}
	for name, oldDefs := range oldByName {
		newDefs := byName[name]
		if len(oldDefs) != len(newDefs) {
			continue
		}
		valid := true
		for _, oldIdx := range oldDefs {
			var dst *plan.IndexDef
			for _, idx := range newDefs {
				if oldIdx.IndexTableName != "" && oldIdx.IndexTableName == idx.IndexTableName && oldIdx.IndexAlgoTableType == idx.IndexAlgoTableType && slices.Equal(oldIdx.Parts, idx.Parts) {
					dst = idx
					break
				}
			}
			if dst == nil || dst.Unique != oldIdx.Unique || dst.IndexAlgo != oldIdx.IndexAlgo || dst.IndexAlgoParams != oldIdx.IndexAlgoParams {
				valid = false
				break
			}
			oldInclude, err := indexDefIncludedColumns(oldIdx)
			if err != nil {
				return nil, err
			}
			newInclude, err := indexDefIncludedColumns(dst)
			if err != nil {
				return nil, err
			}
			if !slices.Equal(oldInclude, newInclude) {
				valid = false
				break
			}
			for _, part := range append(slices.Clone(dst.Parts), newInclude...) {
				name := catalog.ResolveAlias(part)
				// The complete locator was proved above, including hidden composite
				// and fake keys that are absent from the user-column projection.
				if catalog.IsAlias(part) || catalog.IsFakePkName(name) {
					continue
				}
				src := alterCopySourceColumn(old, target, ctx, name, preserved)
				if src == nil || !strings.EqualFold(src.Name, name) {
					valid = false
					break
				}
			}
			if !valid {
				break
			}
		}
		if valid {
			clone[name] = true
		}
	}
	return clone, nil
}
