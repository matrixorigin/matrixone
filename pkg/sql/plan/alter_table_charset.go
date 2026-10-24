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
	"github.com/matrixorigin/matrixone/pkg/common/collation"
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

// Keep the authored target separate from normalized database metadata: a
// canonical spelling reconstructed from a legacy alias is not a new request.
type alterCharsetConversionTarget struct {
	characterSet string
	collation    string
}

func alterTableCharsetConversion(ctx CompilerContext, options []tree.AlterTableOption) (*alterCharsetConversionTarget, error) {
	var conversion *alterCharsetConversionTarget
	for _, option := range options {
		opt, ok := option.(*tree.TableOptionCharset)
		if !ok || !opt.Convert || opt.NonCharsetSyntax {
			continue
		}
		request := []tree.CreateOption{&tree.CreateOptionCharset{Charset: opt.Charset}}
		if opt.Collate != "" {
			request = append(request, &tree.CreateOptionCollate{Collate: opt.Collate})
		}
		normalized, err := NormalizeDatabaseDefaults(ctx.GetContext(), request, "utf8mb4_general_ci")
		if err != nil {
			return nil, err
		}
		conversion = &alterCharsetConversionTarget{
			characterSet: normalized.CharacterSet,
			collation:    normalized.Collation,
		}
		if opt.Collate != "" {
			conversion.collation = opt.Collate
		} else {
			// A charset-only request retains the legacy default in both the
			// target preflight and the later MODIFY replay. Do not synthesize
			// a canonical explicit request that would acquire revision 1.
			switch normalized.CollationId {
			case uint32(collation.UTF8MB4BinIdentity):
				conversion.collation = "utf32_bin"
			case uint32(collation.UTF8MB4GeneralCIIdentity):
				conversion.collation = "utf8mb4_0900_ai_ci"
			}
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
	table.CollationVersion, err = tableDefaultCollationRevision(ctx.GetContext(), defaults, identity)
	if err != nil {
		return err
	}
	if table.KeyFormat == collation.KeyFormatV1 && table.CollationVersion == 1 && !types.IsUnicodeCollation(uint8(identity)) {
		return moerr.NewNotSupported(ctx.GetContext(), "native PAD table default with versioned unique indexes is disabled")
	}
	// V1 physical unique keys also own this schema marker; changing only
	// inheritance must not revoke optimizer-pruned native-unique table shells.
	if types.IsUnicodeCollation(uint8(identity)) || table.KeyFormat == collation.KeyFormatV1 {
		table.CollationVersion = uint32(types.CollationVersionV1)
	}
	return nil
}

// Reuse MODIFY's type/default/generated/constraint binding rather than changing
// only the Type tag. COPY owns row validation, index rebuild, publication and
// failure cleanup. The parsed replay schema and its added attributes stay local.
func convertAlterTableCharacterColumns(ctx CompilerContext, alter *plan.AlterTable, state *AlterTableContext, conversion *alterCharsetConversionTarget) error {
	_, parsed, err := constructCreateTableSQLForReplay(ctx, alter.CopyTableDef, nil, true, nil, true, nil)
	if err != nil {
		return err
	}
	defer parsed.Free()
	create, ok := parsed.(*tree.CreateTable)
	if !ok {
		return moerr.NewInternalError(ctx.GetContext(), "invalid character set conversion schema")
	}
	// Select replay against the complete destination schema, not a partially
	// converted prefix. Retain unaffected typed subtrees while rebinding
	// operations whose character inputs acquire a new domain.
	source := DeepCopyTableDef(alter.CopyTableDef, true)
	target := DeepCopyTableDef(source, true)
	for _, definition := range create.Defs {
		column, ok := definition.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		old := FindColumn(target.Cols, column.Name.ColName())
		if old == nil {
			return moerr.NewInternalError(ctx.GetContext(), "missing character set conversion column")
		}
		switch types.T(old.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			column.Attributes = append(column.Attributes,
				tree.NewAttributeCharset(conversion.characterSet),
				tree.NewAttributeCollate(conversion.collation))
			old.Typ, err = getColumnTypeFromAst(ctx.GetContext(), column, target.DefaultCharset, tableColumnCollationRevision(target), nil)
			if err != nil {
				return err
			}
		}
	}
	replay := ddlReplayForTable(WithPersistedDDLReplay(ctx.GetContext(), alter.CopyTableDef, target), target.Name)
	if replay.err != nil {
		// Never recover a failed typed adaptation by re-authoring the whole
		// persisted expression using the current session.
		return replay.err
	}
	conversionCtx := &charsetConversionCompilerContext{CompilerContext: ctx, replay: replay}
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
		if _, err = ModifyColumn(conversionCtx, alter, &tree.AlterTableModifyColumnClause{NewColumn: column}, state); err != nil {
			return err
		}
		if _, exists := state.changColDefMap[oldID]; exists {
			state.changColDefMap[oldID] = FindColumn(alter.CopyTableDef.Cols, column.Name.ColName())
		}
	}
	return nil
}
