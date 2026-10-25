// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type restoreSchemaInspectionKey struct{}

// WithRestoreSchemaInspection allows restore owners to inspect source DDL for
// dependency sorting. The returned SQL is NOT a standalone backup: tables must
// be restored by typed CLONE, and views need their separate source ViewData.
// This marker changes neither binding generation nor read/write admission.
func WithRestoreSchemaInspection(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, restoreSchemaInspectionKey{}, true)
}

func isRestoreSchemaInspection(ctx CompilerContext) bool {
	if ctx == nil || ctx.GetContext() == nil {
		return false
	}
	inspection, _ := ctx.GetContext().Value(restoreSchemaInspectionKey{}).(bool)
	return inspection
}

func standaloneCollationSQLDisabled(ctx context.Context) error {
	return moerr.NewNotSupported(ctx, "collation binding provenance cannot be encoded by standalone SQL; use typed snapshot/clone restore")
}

// RequireLegacySQLMetadata checks complete source metadata before a legacy-only
// export consumer normalizes or removes fields. It is not a claim that arbitrary
// Createsql can be rebound: the final SQL still needs generation-safe rendering.
func RequireLegacySQLMetadata(ctx context.Context, table *TableDef) error {
	if err := planpb.RequireLegacyCollations(table); err != nil {
		return err
	}
	if table == nil || table.ViewSql == nil {
		return nil
	}
	var data ViewData
	if err := json.Unmarshal([]byte(table.ViewSql.View), &data); err != nil {
		return err
	}
	return requireStandaloneViewCollations(ctx, data)
}

func requireStandaloneViewCollations(ctx context.Context, data ViewData) error {
	if err := requireViewCollationMarker(ctx, data); err != nil {
		return err
	}
	if data.RequiredProtocolVersion != nil && *data.RequiredProtocolVersion < 0 {
		return moerr.NewInvalidInput(ctx, "persisted view protocol version must not be negative")
	}
	if data.CollationBindingRevision != 0 ||
		(data.RequiredProtocolVersion != nil && *data.RequiredProtocolVersion >= defines.MORPCVersion132) {
		return standaloneCollationSQLDisabled(ctx)
	}
	sqlMode := legacyViewParserSQLMode
	if data.SQLMode != nil {
		sqlMode = *data.SQLMode
	}
	return requireStandaloneSQLCollationExpressions(ctx, data.Stmt, sqlMode)
}

func bindingSensitiveCollationName(name string) bool {
	d, ok := collation.Lookup(name)
	return ok && (d.Semantics == collation.GeneralCI || d.Semantics == collation.BinPadSpace) &&
		(d.LegacyIdentity == collation.UTF8MB4GeneralCIIdentity || d.LegacyIdentity == collation.UTF8MB4BinIdentity)
}

// RequireStandaloneLegacySQL guards the raw-SQL fallback when a legacy export
// cannot reconstruct its declarations from complete typed metadata.
func RequireStandaloneLegacySQL(ctx context.Context, sql string) error {
	return requireStandaloneSQLCollations(ctx, sql, "", true)
}

func requireStandaloneSQLCollationExpressions(ctx context.Context, sql, sqlMode string) error {
	return requireStandaloneSQLCollations(ctx, sql, sqlMode, false)
}

func requireStandaloneSQLCollations(ctx context.Context, sql, sqlMode string, declarations bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Avoid parsing unrelated, unescaped legacy output. name_string also
	// accepts SQL strings: escapes can hide the canonical spelling until the
	// lexer decodes it. Conservatively parse escaped SQL; only the AST decides
	// whether a COLLATE exists (not a quoted value or comment).
	lower := strings.ToLower(sql)
	candidate := strings.ContainsRune(sql, '\\')
	for _, definition := range collation.Definitions() {
		if !bindingSensitiveCollationName(definition.Name) {
			continue
		}
		if strings.Contains(lower, definition.Name) ||
			(strings.HasPrefix(definition.Name, "utf8_") && strings.Contains(lower, "utf8mb3_"+strings.TrimPrefix(definition.Name, "utf8_"))) {
			candidate = true
			break
		}
	}
	if !candidate {
		return nil
	}
	statements, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, sql, 1, sqlMode)
	if err != nil {
		return err
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()
	seen := make(map[collationExprVisit]struct{})
	for _, statement := range statements {
		if hasBindingSensitiveCollate(reflect.ValueOf(statement), seen, declarations) {
			return standaloneCollationSQLDisabled(ctx)
		}
	}
	return nil
}

type collationExprVisit struct {
	typ reflect.Type
	ptr uintptr
}

// Like the parameter-marker AST audit, do not rely on Expr.Accept: several
// valid statement/subquery nodes have incomplete visitors. Borrow without
// mutating or invoking methods, deduplicating shared/cyclic pointers.
func hasBindingSensitiveCollate(value reflect.Value, seen map[collationExprVisit]struct{}, declarations bool) bool {
	if !value.IsValid() {
		return false
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			return hasBindingSensitiveCollate(value.Elem(), seen, declarations)
		}
	case reflect.Pointer:
		if value.IsNil() {
			return false
		}
		if value.CanInterface() {
			var name string
			switch node := value.Interface().(type) {
			case *tree.CollateExpr:
				name = node.Collation
			case *tree.AttributeCollate:
				if declarations {
					name = node.Collate
				}
			case *tree.TableOptionCollate:
				if declarations {
					name = node.Collate
				}
			case *tree.TableOptionCharset:
				if declarations {
					name = node.Collate
				}
			}
			if bindingSensitiveCollationName(name) {
				return true
			}
		}
		key := collationExprVisit{typ: value.Type(), ptr: value.Pointer()}
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
		return hasBindingSensitiveCollate(value.Elem(), seen, declarations)
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if hasBindingSensitiveCollate(value.Field(i), seen, declarations) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if hasBindingSensitiveCollate(value.Index(i), seen, declarations) {
				return true
			}
		}
	}
	return false
}
