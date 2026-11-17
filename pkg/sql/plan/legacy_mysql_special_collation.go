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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type legacyEnumSetColumn struct {
	ordinal             int
	name                string
	typ                 planpb.Type
	inheritTableCharset bool
}

// RecoverLegacyEnumSetCollation restores display-label collation metadata that
// older catalog writers omitted. Historical CREATE SQL is used only for an
// unchanged physical table and a unique durable column Seqnum. The historical
// physical type and labels must also match, so stale SQL cannot redefine a
// column. Recovery mutates only the planner-owned TableDef clone.
//
// A declaration without an explicit table or column identity inherits the
// current durable table default. If that default is also absent, or the stored
// SQL is stale, ambiguous, or a direct CREATE LIKE statement, recovery is a
// safe no-op and preserves the legacy behavior.
func RecoverLegacyEnumSetCollation(ctx context.Context, tableDef *planpb.TableDef) {
	if tableDef == nil || !hasAuthoritativeLegacyCreate(tableDef) || tableDef.Createsql == "" ||
		!isLegacyTinyTextTableKind(tableDef.TableType) || !hasLegacyEnumSetCollation(tableDef) {
		return
	}
	lowerCreateSQL := strings.ToLower(tableDef.Createsql)
	if !strings.Contains(lowerCreateSQL, "enum") && !strings.Contains(lowerCreateSQL, "set") {
		return
	}
	evidence, err := parseCachedLegacyTinyTextEvidence(ctx, tableDef.Createsql)
	if err != nil {
		return
	}
	for _, historical := range evidence.enumSetColumns {
		candidateIndex := -1
		sequenceNumber := uint32(historical.ordinal - 1)
		for index, column := range tableDef.Cols {
			if column == nil || column.Hidden || column.Seqnum != sequenceNumber {
				continue
			}
			if candidateIndex != -1 {
				candidateIndex = -1
				break
			}
			candidateIndex = index
		}
		if candidateIndex == -1 {
			continue
		}
		candidate := tableDef.Cols[candidateIndex]
		if candidate.Typ.Charset != uint32(types.CharsetLegacy) {
			continue
		}
		recovered := historical.typ
		if recovered.Id != candidate.Typ.Id || recovered.Enumvalues != candidate.Typ.Enumvalues {
			continue
		}
		if historical.inheritTableCharset {
			if tableDef.DefaultCharset == uint32(types.CharsetLegacy) {
				continue
			}
			applyTextCharsetToPlanType(&recovered, tableDef.DefaultCharset)
		}
		if recovered.Charset == uint32(types.CharsetLegacy) {
			continue
		}
		cloned := *candidate
		cloned.Typ.Charset = recovered.Charset
		cloned.Typ.CollationVersion = recovered.CollationVersion
		tableDef.Cols[candidateIndex] = &cloned
	}
}

func hasLegacyEnumSetCollation(tableDef *planpb.TableDef) bool {
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden && isEnumOrSetPlanType(&column.Typ) &&
			column.Typ.Charset == uint32(types.CharsetLegacy) {
			return true
		}
	}
	return false
}

func legacyEnumSetColumnsFromCreate(
	ctx context.Context,
	stmt *tree.CreateTable,
) ([]legacyEnumSetColumn, error) {
	tableCharset, explicitTableCharset, err := legacyExplicitTableCharset(ctx, stmt.Options)
	if err != nil {
		// ENUM/SET evidence is best-effort. An unsupported historical identity
		// must not disable independent legacy TEXT-family recovery.
		return nil, nil
	}
	if !explicitTableCharset {
		tableCharset = uint32(types.CharsetLegacy)
	}
	columns := make([]legacyEnumSetColumn, 0)
	ordinal := 0
	for _, def := range stmt.Defs {
		column, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		ordinal++
		astType, ok := column.Type.(*tree.T)
		if !ok {
			continue
		}
		switch defines.MysqlType(astType.InternalType.Oid) {
		case defines.MYSQL_TYPE_ENUM, defines.MYSQL_TYPE_SET:
		default:
			continue
		}
		typ, err := getColumnTypeFromAst(ctx, column, tableCharset, nil)
		if err != nil {
			continue
		}
		if !isEnumOrSetPlanType(&typ) {
			continue
		}
		columns = append(columns, legacyEnumSetColumn{
			ordinal:             ordinal,
			name:                column.Name.ColName(),
			typ:                 typ,
			inheritTableCharset: !explicitTableCharset && !legacyColumnHasCharsetOverride(column.Attributes),
		})
	}
	return columns, nil
}

func legacyColumnHasCharsetOverride(attributes []tree.ColumnAttribute) bool {
	for _, attribute := range attributes {
		switch attribute.(type) {
		case *tree.AttributeCharset, *tree.AttributeCollate:
			return true
		}
	}
	return false
}

// legacyExplicitTableCharset mirrors the syntax-only portion of
// tableDefaultCharset. An omitted table identity remains unresolved here so a
// stored statement cannot capture a later session's collation_server value.
func legacyExplicitTableCharset(
	ctx context.Context,
	options []tree.TableOption,
) (uint32, bool, error) {
	tableCharset := uint32(types.CharsetUTF8)
	tableCollation := uint32(types.CharsetUTF8)
	var tableCharsetName string
	var tableCollationName string
	hasTableCollation := false
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.TableOptionCharset:
			tableCharsetName = opt.Charset
			var ok bool
			tableCharset, ok = charsetForName(opt.Charset)
			if !ok {
				return 0, false, moerr.NewInvalidInputf(ctx, "unsupported character set '%s'", opt.Charset)
			}
		case *tree.TableOptionCollate:
			tableCollationName = opt.Collate
			var ok bool
			tableCollation, ok = collationForName(opt.Collate)
			if !ok {
				return 0, false, unsupportedCollationError(ctx, opt.Collate)
			}
			hasTableCollation = true
		}
	}
	if tableCharsetName != "" && tableCollationName != "" &&
		!charsetAndCollationCompatible(tableCharsetName, tableCollationName) {
		return 0, false, moerr.NewInvalidInputf(ctx,
			"COLLATION '%s' is not valid for CHARACTER SET '%s'",
			tableCollationName, tableCharsetName)
	}
	if hasTableCollation {
		return tableCollation, true, nil
	}
	if tableCharsetName != "" {
		return tableCharset, true, nil
	}
	return 0, false, nil
}
