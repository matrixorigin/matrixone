// Copyright 2023 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildAlterRenameIndexes(ctx context.Context, stmt *tree.AlterTable, db string, table *TableDef) (*Plan, error) {
	if table.TableType != catalog.SystemOrdinaryRel || table.IsTemporary ||
		table.Partition != nil || features.IsPartitioned(table.FeatureFlag) || stmt.PartitionOption != nil {
		return nil, moerr.NewNotSupported(ctx, "RENAME INDEX requires a permanent, nonpartitioned base table")
	}
	if err := rejectVersionedIndexMutation(ctx, table); err != nil {
		return nil, err
	}

	alter := &plan.AlterTable{
		Database:      db,
		TableDef:      table,
		AlgorithmType: plan.AlterTable_INPLACE,
		RawSQL:        tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithQuoteIdentifier()),
	}
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.AlterTableRenameIndexClause:
			alter.Actions = append(alter.Actions, &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_RenameIndex{
					RenameIndex: &plan.AlterTableRenameIndex{OldName: opt.OldName, NewName: opt.NewName},
				},
			})
		case *tree.AlterOptionAlgorithm:
			switch strings.ToUpper(opt.Type) {
			case "DEFAULT", "INPLACE":
			default:
				return nil, moerr.NewNotSupported(ctx, "RENAME INDEX requires ALGORITHM=INPLACE")
			}
		case *tree.AlterOptionLock:
			switch strings.ToUpper(opt.Type) {
			case "DEFAULT", "EXCLUSIVE":
			default:
				return nil, moerr.NewNotSupported(ctx, "RENAME INDEX requires LOCK=EXCLUSIVE")
			}
		default:
			return nil, moerr.NewNotSupported(ctx, "RENAME INDEX combined with other ALTER operations")
		}
	}
	if _, err := ResolveIndexRenames(ctx, table, alter.Actions); err != nil {
		return nil, err
	}
	return &Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		DdlType:    plan.DataDefinition_ALTER_TABLE,
		Definition: &plan.DataDefinition_AlterTable{AlterTable: alter},
	}}}, nil
}

// ResolveIndexRenames binds aliases left to right and returns only the final
// changes, keyed by exact original catalog name. The definitions are read-only.
func ResolveIndexRenames(ctx context.Context, table *TableDef, actions []*plan.AlterTable_Action) (map[string]string, error) {
	byName := make(map[string]*plan.IndexDef, len(table.Indexes))
	for _, index := range table.Indexes {
		byName[indexNameKey(index.IndexName)] = index
	}
	renames := make(map[string]string, len(actions))
	for _, action := range actions {
		rename := action.GetRenameIndex()
		if rename == nil {
			return nil, moerr.NewInternalError(ctx, "invalid RENAME INDEX plan")
		}
		for _, name := range []string{rename.OldName, rename.NewName} {
			if err := validateIdentifier(ctx, name); err != nil {
				return nil, err
			}
			if name == "" || strings.ContainsRune(name, 0) || strings.HasSuffix(name, " ") ||
				IndexNamesEqual(name, "PRIMARY") {
				return nil, moerr.NewErrWrongNameForIndex(ctx, name)
			}
		}
		oldKey, newKey := indexNameKey(rename.OldName), indexNameKey(rename.NewName)
		index := byName[oldKey]
		if index == nil {
			return nil, moerr.NewErrCantDropFieldOrKey(ctx, rename.OldName)
		}
		if byName[newKey] != nil && oldKey != newKey {
			return nil, moerr.NewDuplicateKey(ctx, rename.NewName)
		}
		if !index.TableExist || index.IndexTableName == "" ||
			(!catalog.IsNullIndexAlgo(index.IndexAlgo) && catalog.ToLower(index.IndexAlgo) != catalog.MoIndexBTreeAlgo.ToString()) ||
			functionalIndexColumn(table, index) != nil {
			return nil, moerr.NewNotSupported(ctx, "RENAME INDEX requires an ordinary column index")
		}
		delete(byName, oldKey)
		byName[newKey] = index
		if rename.NewName == index.IndexName {
			delete(renames, index.IndexName)
		} else {
			renames[index.IndexName] = rename.NewName
		}
	}
	return renames, nil
}
