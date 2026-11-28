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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type restoredViewDefinitionKey struct{}

type restoredViewDefinition struct {
	database, name, definition, createSQL string
}

// WithRestoredViewDefinition binds the original persisted metadata to one
// restore-owned CREATE VIEW. It neither changes session variables nor bypasses
// the caller's persisted-protocol admission and restore authorization checks.
func WithRestoredViewDefinition(ctx context.Context, database, name, definition, createSQL string) context.Context {
	if definition == "" {
		// An absent legacy payload must not acquire the restore session's
		// creation environment and become falsely qualified for S2.
		definition = "{}"
	}
	return context.WithValue(ctx, restoredViewDefinitionKey{}, restoredViewDefinition{database, name, definition, createSQL})
}

// RestoredViewLowerCaseTableNames restores parser policy only for the exact
// replay statement. Nested catalog SQL and ordinary statements retain their own
// policy; neither the session nor global variables are modified.
func RestoredViewLowerCaseTableNames(ctx context.Context, sql string, fallback int64) (int64, error) {
	if ctx == nil {
		return fallback, nil
	}
	source, ok := ctx.Value(restoredViewDefinitionKey{}).(restoredViewDefinition)
	if !ok || source.createSQL == "" || source.createSQL != sql {
		return fallback, nil
	}
	var data ViewData
	if err := json.Unmarshal([]byte(source.definition), &data); err != nil {
		return 0, err
	}
	return restoredViewLowerCase(ctx, data, fallback)
}

func restoredViewLowerCase(ctx context.Context, data ViewData, fallback int64) (int64, error) {
	if data.LowerCaseTableNames == nil {
		return fallback, nil
	}
	mode := *data.LowerCaseTableNames
	if mode < 0 || mode > 2 {
		return 0, moerr.NewInvalidInput(ctx, "invalid restored lower_case_table_names")
	}
	return mode, nil
}

// restoredLegacyInnerWindowInferenceInput reproduces the released server's
// CREATE inference boundary for marker-free restored definitions generally:
// the released genViewTableDef switch binds the immediate parenthesized child
// in every paren-form shape (`case *tree.ParenSelect` -> bind(s.Select), with
// no condition on the outer window, a WITH or any inner clause - see
// build_ddl.go:171-182 at the v4.2.9 release commit 6c05e42), while every
// non-parenthesized form binds the whole statement. The restored
// schema/dependency inference selects that same inner input, so an
// extra-grouping layer or an authored WITH inside the body resolves exactly
// as the release did; every other shape keeps the complete defining select.
func restoredLegacyInnerWindowInferenceInput(ctx CompilerContext, database, name string, stmt *tree.Select) *tree.Select {
	inner, ok := restoredLegacyInnerWindow(ctx, database, name, stmt)
	if !ok || inner == nil {
		return stmt
	}
	return inner
}

func restoredLegacyInnerWindow(ctx CompilerContext, database, name string, stmt *tree.Select) (*tree.Select, bool) {
	if ctx == nil || ctx.GetContext() == nil || stmt == nil {
		return nil, false
	}
	source, ok := ctx.GetContext().Value(restoredViewDefinitionKey{}).(restoredViewDefinition)
	if !ok || source.database != database || source.name != name || source.createSQL == "" {
		return nil, false
	}
	var data ViewData
	if err := json.Unmarshal([]byte(source.definition), &data); err != nil || data.CTEScopeProtocol != nil {
		return nil, false
	}
	// The released server's genViewTableDef binds the immediate parenthesized
	// child for CREATE VIEW inference in every paren-form shape: its switch arm
	// is `case *tree.ParenSelect` -> bind(s.Select) with no condition on the
	// outer window, a WITH, lock/export/INTO state or any inner clause, while
	// the non-paren forms bind the whole statement (build_ddl.go:171-182 at the
	// v4.2.9 release commit 6c05e42). This guard mirrors that boundary for
	// marker-free restored definitions: a *tree.ParenSelect body hands the
	// immediate child to the schema/dependency inference whatever clauses
	// either level carries, and every other shape keeps the complete defining
	// select so current-generation validation stays whole. The authentic
	// release-authored extra-grouping capture proves the immediate child may
	// itself be a transparent paren layer whose window binds through, and the
	// release rejects the WITH-paren-body spelling outright (1064), so no
	// marker-free definition of that shape can exist.
	paren, ok := stmt.Select.(*tree.ParenSelect)
	if !ok || paren.Select == nil {
		return nil, false
	}
	return paren.Select, true
}

func restoredViewCreationContext(ctx CompilerContext, database, name string) (CompilerContext, error) {
	if ctx.GetContext() == nil {
		return ctx, nil
	}
	source, ok := ctx.GetContext().Value(restoredViewDefinitionKey{}).(restoredViewDefinition)
	if !ok || source.database != database || source.name != name {
		return ctx, nil
	}
	var fields map[string]json.RawMessage
	var data ViewData
	if err := json.Unmarshal([]byte(source.definition), &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "invalid restored view definition")
	}
	if err := json.Unmarshal([]byte(source.definition), &data); err != nil {
		return nil, err
	}
	lower, err := restoredViewLowerCase(ctx.GetContext(), data, ctx.GetLowerCaseTableNames())
	if err != nil {
		return nil, err
	}
	db := ctx.DefaultDatabase()
	if value, present := fields["DefaultDatabase"]; present && string(value) != "null" {
		db = data.DefaultDatabase
	}
	// The restored definition is re-authored in the generation it was authored
	// under: a release-era definition (persisted without cte_scope_protocol)
	// keeps the legacy resolution for its re-inferred columns and dependencies,
	// while a definition recorded with the current generation stays corrected.
	return &viewRegenerationContext{CompilerContext: ctx, defaultDatabase: db,
		rootSQL: ctx.GetRootSql(), lowerCaseTableNames: lower,
		legacyCTEScope: data.CTEScopeProtocol == nil}, nil
}

func preserveRestoredViewDefinition(ctx context.Context, database, name, generated string) (string, error) {
	if ctx == nil {
		return generated, nil
	}
	source, ok := ctx.Value(restoredViewDefinitionKey{}).(restoredViewDefinition)
	if !ok || source.database != database || source.name != name {
		return generated, nil
	}
	var original, rebuilt map[string]json.RawMessage
	var originalData, rebuiltData ViewData
	if err := json.Unmarshal([]byte(source.definition), &original); err != nil {
		return "", err
	}
	if original == nil {
		return "", moerr.NewInvalidInput(ctx, "invalid restored view definition")
	}
	if err := json.Unmarshal([]byte(source.definition), &originalData); err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(generated), &rebuilt); err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(generated), &rebuiltData); err != nil {
		return "", err
	}
	if rebuilt == nil {
		return "", moerr.NewInvalidInput(ctx, "invalid rebuilt view definition")
	}
	// Rebind target SQL, object identities and dependencies. Everything else,
	// including absent legacy creation fields and unknown future fields, belongs
	// to the persisted definition, not to the restoring session's defaults.
	for _, key := range []string{"Stmt", "definition", "dependencies", "required_protocol_version"} {
		if value, exists := rebuilt[key]; exists {
			original[key] = value
		} else {
			delete(original, key)
		}
	}
	// Restore may remap database names, but must not turn an unfrozen legacy
	// star into evidence of a known creation-time projection. Retain the actual
	// replay input, before the CREATE planner expands against the target schema.
	if source.createSQL != "" {
		original["Stmt"], _ = json.Marshal(source.createSQL)
	}
	if originalData.RequiredProtocolVersion != nil &&
		(rebuiltData.RequiredProtocolVersion == nil || *originalData.RequiredProtocolVersion > *rebuiltData.RequiredProtocolVersion) {
		original["required_protocol_version"], _ = json.Marshal(*originalData.RequiredProtocolVersion)
	}
	result, err := json.Marshal(original)
	return string(result), err
}
