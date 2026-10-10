// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func charsetTableOptions(option *tree.TableOptionCharset) []tree.TableOption {
	options := []tree.TableOption{option}
	if option.Collate != "" {
		options = append(options, &tree.TableOptionCollate{Collate: option.Collate})
	}
	return options
}

// DEFAULT options own the final table default. CONVERT owns the character
// columns, including ADD/MODIFY clauses independent of their textual order.
func alterTableCharsetDefaultOptions(options []tree.AlterTableOption) []tree.TableOption {
	var defaults []tree.TableOption
	var conversion *tree.TableOptionCharset
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.TableOptionCharset:
			if opt.NonCharsetSyntax {
				continue
			}
			if opt.Convert {
				conversion = opt
			} else {
				defaults = append(defaults, charsetTableOptions(opt)...)
			}
		case *tree.TableOptionCollate:
			defaults = append(defaults, opt)
		}
	}
	if len(defaults) == 0 && conversion != nil {
		return charsetTableOptions(conversion)
	}
	return defaults
}

func alterTableCharsetConversion(ctx CompilerContext, options []tree.AlterTableOption) (*plan.DatabaseDefaults, error) {
	var conversion *plan.DatabaseDefaults
	for _, option := range options {
		opt, ok := option.(*tree.TableOptionCharset)
		if !ok || !opt.Convert || opt.NonCharsetSyntax {
			continue
		}
		request := []tree.CreateOption{&tree.CreateOptionCharset{Charset: opt.Charset}}
		if opt.Collate != "" {
			request = append(request, &tree.CreateOptionCollate{Collate: opt.Collate})
		}
		var err error
		conversion, err = NormalizeDatabaseDefaults(ctx.GetContext(), request, "utf8mb4_general_ci")
		if err != nil {
			return nil, err
		}
	}
	return conversion, nil
}

func applyAlterTableCharsetDefault(ctx CompilerContext, table *plan.TableDef, options []tree.AlterTableOption) error {
	defaults := alterTableCharsetDefaultOptions(options)
	if len(defaults) == 0 {
		return nil
	}
	identity, err := tableDefaultCharset(ctx, defaults)
	if err != nil {
		return err
	}
	table.DefaultCharset = identity
	table.CollationVersion = uint32(types.CollationVersionLegacy)
	if types.IsUnicodeCollation(uint8(identity)) {
		table.CollationVersion = uint32(types.CollationVersionV1)
	}
	return nil
}

// Reuse MODIFY's type/default/generated/constraint binding rather than changing
// only the Type tag. COPY owns row validation, index rebuild, publication and
// failure cleanup. The parsed replay schema and its added attributes stay local.
func convertAlterTableCharacterColumns(ctx CompilerContext, alter *plan.AlterTable, state *AlterTableContext, conversion *plan.DatabaseDefaults) error {
	_, parsed, err := constructCreateTableSQL(ctx, alter.CopyTableDef, nil, true, nil, true, nil)
	if err != nil {
		return err
	}
	defer parsed.Free()
	create, ok := parsed.(*tree.CreateTable)
	if !ok {
		return moerr.NewInternalError(ctx.GetContext(), "invalid character set conversion schema")
	}
	for _, definition := range create.Defs {
		column, ok := definition.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		old := FindColumn(alter.CopyTableDef.Cols, column.Name.ColName())
		if old == nil {
			return moerr.NewInternalError(ctx.GetContext(), "missing character set conversion column")
		}
		switch types.T(old.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
		default:
			// Existing binary strings and non-character values are not converted.
			continue
		}
		oldID := old.ColId
		column.Attributes = append(column.Attributes,
			tree.NewAttributeCharset(conversion.CharacterSet),
			tree.NewAttributeCollate(conversion.Collation))
		if _, err = ModifyColumn(ctx, alter, &tree.AlterTableModifyColumnClause{NewColumn: column}, state); err != nil {
			return err
		}
		if _, exists := state.changColDefMap[oldID]; exists {
			state.changColDefMap[oldID] = FindColumn(alter.CopyTableDef.Cols, column.Name.ColName())
		}
	}
	return nil
}
