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
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// requireViewSchemaCreationContext rejects unknown historical binding context before
// any root or nested binding/memo lookup. Read only the required metadata, not
// the SQL text; the ordinary public binder retains its compatibility fallback.
func requireViewSchemaCreationContext(ctx context.Context, definition string) error {
	var metadata struct {
		DefaultDatabase     string
		LowerCaseTableNames *int64 `json:"lower_case_table_names"`
	}
	if err := json.Unmarshal([]byte(definition), &metadata); err != nil {
		return err
	}
	if metadata.LowerCaseTableNames == nil {
		return moerr.NewNotSupported(ctx, "LEGACY_CONTEXT_UNAVAILABLE: persisted View has no creation-time lower_case_table_names")
	}
	if metadata.DefaultDatabase == "" {
		return moerr.NewNotSupported(ctx, "LEGACY_CONTEXT_UNAVAILABLE: persisted View has no creation-time DefaultDatabase")
	}
	return nil
}

// rejectViewSchemaUnstableStar checks every query block in a persisted View,
// including CTEs, derived tables and subqueries. COUNT(*) is not an output
// projection star. A legacy Cols list cannot reconstruct the original column
// mapping after the source schema changes. This gate is exclusive to the
// opt-in View schema request; ordinary SQL keeps its existing behavior.
func rejectViewSchemaUnstableStar(request *ViewSchemaRequest, statement *tree.Select) error {
	if statement == nil {
		return nil
	}
	pending := []reflect.Value{reflect.ValueOf(statement)}
	visited := make(map[treeClonePointer]struct{})
	for visits := 0; len(pending) > 0; visits++ {
		if visits%256 == 0 {
			if err := request.check(); err != nil {
				return err
			}
		}
		last := len(pending) - 1
		value := pending[last]
		pending = pending[:last]
		for value.IsValid() && value.Kind() == reflect.Interface {
			if value.IsNil() || !value.CanInterface() {
				break
			}
			value = value.Elem()
		}
		if !value.IsValid() || (value.Kind() == reflect.Interface && value.IsNil()) {
			continue
		}
		if value.Kind() == reflect.Pointer {
			if value.IsNil() || !value.CanInterface() {
				continue
			}
			key := treeClonePointer{typ: value.Type(), ptr: value.Pointer()}
			if _, ok := visited[key]; ok {
				continue
			}
			visited[key] = struct{}{}
			// SampleExpr keeps its expressions in a private field. Follow the
			// public accessor so a subquery inside SAMPLE cannot hide its own
			// unfrozen output star from the query-block check below.
			if sample, ok := value.Interface().(*tree.SampleExpr); ok {
				columns, _ := sample.GetColumns()
				for _, column := range columns {
					pending = append(pending, reflect.ValueOf(column))
				}
			}
			if clause, ok := value.Interface().(*tree.SelectClause); ok && selectClauseOutputHasStar(clause) {
				return moerr.NewNotSupported(request.workCtx, "LEGACY_STAR_UNAVAILABLE: persisted View projection has an unfrozen star")
			}
			value = value.Elem()
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if field := value.Field(i); field.CanInterface() {
					pending = append(pending, field)
				}
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < value.Len(); i++ {
				pending = append(pending, value.Index(i))
			}
		}
	}
	return request.check()
}
