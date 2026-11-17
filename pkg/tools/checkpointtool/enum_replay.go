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

package checkpointtool

import (
	"context"
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// RestoreTableCollation validates the typed schema before SQL-script export.
// Raw CSV export does not make this replay guarantee and does not call it.
// A read-only external relation cannot inherit an ENUM through ALTER and
// retains its established non-ENUM rendering without a table-default clause.
func RestoreTableCollation(ctx context.Context, schema *TableSchema, readOnlyExternal bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if schema == nil || len(schema.Columns) == 0 {
		return "", moerr.NewNotSupported(ctx, "checkpoint restore requires resolved column metadata")
	}
	reject := func(reason string) (string, error) {
		return "", moerr.NewNotSupportedf(ctx, "checkpoint restore of %s.%s is not supported: %s; use raw CSV export instead", schema.DatabaseName, schema.TableName, reason)
	}
	for _, col := range schema.Columns {
		sqlType := strings.TrimSpace(col.SQLType)
		if i := strings.IndexAny(sqlType, " (\t\r\n"); i >= 0 {
			sqlType = sqlType[:i]
		}
		if (col.NativeType != nil && col.NativeType.Oid == types.T_enum) || strings.EqualFold(sqlType, "ENUM") {
			return reject("ENUM identity, stored ordinal and default replay cannot yet be proved")
		}
	}
	if schema.schemaExtraError != nil {
		_, err := reject("invalid table collation metadata")
		return "", errors.Join(err, schema.schemaExtraError)
	}
	extra := schema.SchemaExtra
	if extra == nil {
		return reject("missing or unrecognized table collation metadata")
	}
	if err := extra.ValidateCollation(); err != nil {
		_, outer := reject("invalid table collation metadata")
		return "", errors.Join(outer, err)
	}
	if len(extra.XXX_unrecognized) != 0 {
		return reject("unknown table metadata fields")
	}
	if extra.EnumUnsupportedDefault != "" {
		return reject("the table has an unsupported ENUM default collation")
	}
	if readOnlyExternal {
		for _, col := range schema.Columns {
			if col.NativeType == nil {
				return reject("an external column has no validated native type")
			}
		}
		return "", nil
	}
	var name string
	switch {
	case extra.DefaultCharset == uint32(collation.UTF8MB4BinIdentity) && extra.CollationVersion == 0:
		name = "utf8mb4_bin"
	case extra.DefaultCharset == uint32(collation.UTF8MB4GeneralCIIdentity) && extra.CollationVersion == 0:
		name = "utf8mb4_general_ci"
	case extra.DefaultCharset == uint32(collation.UTF8MB4UnicodeCIIdentity) && extra.CollationVersion == uint32(collation.RevisionV1):
		name = "utf8mb4_unicode_ci"
	default:
		return reject("the table default has no proven SQL representation")
	}
	if extra.KeyFormat != collation.KeyFormatLegacy || len(extra.Checks) != 0 {
		return reject("index format or CHECK expression replay is not proved")
	}
	for _, key := range schema.UniqueKeys {
		if len(key.Expressions) != 0 {
			return reject("functional index replay is not proved")
		}
	}
	for _, col := range schema.Columns {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if col.NativeType == nil {
			return reject("a column has no validated native type")
		}
		typ := *col.NativeType
		if sqlTypeFromMOType(typ) != col.SQLType {
			return reject("a column's SQL and native types disagree")
		}
		if err := typ.ValidateCollation(); err != nil {
			return "", err
		}
		if col.HasDefault || col.Default != "" || col.OnUpdate != "" || col.Generated != "" {
			return reject("stored expression replay is not proved")
		}
		if col.EnumValues != "" {
			return reject("special-domain replay is not proved")
		}
		switch typ.Oid {
		case types.T_char, types.T_varchar, types.T_text:
			if uint32(typ.Charset) != extra.DefaultCharset || uint32(typ.CollationVersion) != extra.CollationVersion {
				return reject("a text column differs from the table default")
			}
		case types.T_binary, types.T_varbinary, types.T_blob:
			// Their SQL family remains binary under a text table default.
		}
	}
	return name, nil
}

// A partial or duplicate header is not an authority for a persisted type or
// table default. Raw catalog-display fallbacks keep their existing behavior.
func uniqueCatalogColumnIndex(view *LogicalTableView, name string) int {
	if view == nil {
		return -1
	}
	found := -1
	for i := logicalViewDataOffset(view); i < len(view.Headers); i++ {
		if view.Headers[i] != name {
			continue
		}
		if found >= 0 {
			return -1
		}
		found = i - logicalViewDataOffset(view)
	}
	return found
}
