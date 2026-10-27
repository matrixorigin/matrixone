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
	return &viewRegenerationContext{CompilerContext: ctx, defaultDatabase: db,
		rootSQL: ctx.GetRootSql(), lowerCaseTableNames: lower}, nil
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
