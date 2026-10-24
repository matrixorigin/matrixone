// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/ttl"
)

func applyTTLOption(ctx CompilerContext, def *TableDef, option *tree.TableOptionTTL) error {
	if option.Remove {
		ttl.Set(def, nil)
		return nil
	}
	seconds, err := ttl.Duration(option.Amount, option.Unit)
	if err != nil {
		return moerr.NewInvalidInputf(ctx.GetContext(), "%s", err)
	}
	col := FindColumn(def.Cols, string(option.Column))
	if col == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "TTL column does not exist")
	}
	p := &ttl.Policy{Version: 1, Column: col.Name, Seqnum: col.Seqnum, Seconds: seconds}
	if err := ttl.ValidateColumn(def, p); err != nil {
		return moerr.NewInvalidInputf(ctx.GetContext(), "%s", err)
	}
	ttl.Set(def, p)
	return validateTTLTable(ctx, def)
}
func validateTTLTable(ctx CompilerContext, def *TableDef) error {
	p, err := ttl.Get(def)
	if err != nil {
		return moerr.NewInvalidInputf(ctx.GetContext(), "%s", err)
	}
	if p == nil {
		return nil
	}
	kind := def.TableType
	for _, d := range def.Defs {
		if properties := d.GetProperties(); properties != nil {
			for _, prop := range properties.Properties {
				if prop.Key == catalog.SystemRelAttr_Kind {
					kind = prop.Value
				}
			}
		}
	}
	if def.IsTemporary || kind != catalog.SystemOrdinaryRel || def.ViewSql != nil || def.Partition != nil {
		return moerr.NewNotSupported(ctx.GetContext(), "TTL requires a permanent, ordinary, non-partitioned table")
	}
	if def.DbName != "" && !ttl.IsUserDatabase(def.DbName) {
		return moerr.NewNotSupported(ctx.GetContext(), "TTL on a system database")
	}
	if len(def.RefChildTbls) != 0 {
		return moerr.NewNotSupported(ctx.GetContext(), "TTL on a referenced foreign key parent")
	}
	if err = ttl.ValidateColumn(def, p); err != nil {
		return moerr.NewInvalidInputf(ctx.GetContext(), "%s", err)
	}
	return nil
}

// Keep the first version's TTL mutation standalone, and reject changes to its
// source column until REMOVE TTL. Unrelated DDL retains the persisted policy.
func validateTTLAlter(ctx CompilerContext, def *TableDef, stmt *tree.AlterTable) error {
	p, err := ttl.Get(def)
	if err != nil {
		return err
	}
	ttlOptions := 0
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.TableOptionTTL:
			ttlOptions++
		case *tree.AlterOptionDrop:
			if p != nil && opt.Typ == tree.AlterTableDropColumn && string(opt.Name) == p.Column {
				return moerr.NewInvalidInput(ctx.GetContext(), "REMOVE TTL before dropping its source column")
			}
		case *tree.AlterTableRenameColumnClause:
			if p != nil && opt.OldColumnName.ColName() == p.Column {
				return moerr.NewInvalidInput(ctx.GetContext(), "REMOVE TTL before renaming its source column")
			}
		case *tree.AlterTableModifyColumnClause:
			if p != nil && opt.NewColumn.Name.ColName() == p.Column {
				return moerr.NewInvalidInput(ctx.GetContext(), "REMOVE TTL before modifying its source column")
			}
		case *tree.AlterTableChangeColumnClause:
			if p != nil && opt.OldColumnName.ColName() == p.Column {
				return moerr.NewInvalidInput(ctx.GetContext(), "REMOVE TTL before changing its source column")
			}
		}
	}
	if ttlOptions > 0 && len(stmt.Options) != 1 {
		return moerr.NewInvalidInput(ctx.GetContext(), "TTL changes must be standalone ALTER statements")
	}
	return nil
}
