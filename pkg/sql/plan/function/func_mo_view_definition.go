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

package function

import (
	"context"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// legacyViewDefinitionSQLMode is the parser compatibility default used for
// persisted View definitions that predate recording SQLMode in ViewData.
const legacyViewDefinitionSQLMode = "PIPES_AS_CONCAT"

type persistedViewDefinitionData struct {
	Stmt                string
	Definition          string  `json:"definition,omitempty"`
	SQLMode             *string `json:"sql_mode,omitempty"`
	LowerCaseTableNames *int64  `json:"lower_case_table_names,omitempty"`
}

// builtInViewDefinition returns the frozen parser-derived definition for a
// current View and supplies a parser-aware compatibility read for legacy rows.
// It deliberately does not write catalog data: metadata reads must remain
// bounded, side-effect-free, and independent of the inactive refresh lifecycle.
func builtInViewDefinition(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	definitions := vector.GenerateFunctionStrParameter(parameters[0])
	results := vector.MustFunctionResult[types.Varlena](result)

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			if err := results.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		persisted, isNull := definitions.GetStrValue(row)
		if isNull {
			if err := results.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		definition, ok := viewDefinitionFromPersistedData(proc.Ctx, string(persisted))
		if !ok {
			if err := results.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := results.AppendBytes([]byte(definition), false); err != nil {
			return err
		}
	}
	return nil
}

func viewDefinitionFromPersistedData(ctx context.Context, persisted string) (string, bool) {
	var data persistedViewDefinitionData
	if err := json.Unmarshal([]byte(persisted), &data); err != nil {
		return "", false
	}
	if data.Definition != "" {
		return data.Definition, true
	}
	if data.Stmt == "" {
		return "", false
	}

	lowerCaseTableNames := int64(0)
	if data.LowerCaseTableNames != nil {
		lowerCaseTableNames = *data.LowerCaseTableNames
	}
	parserSQLMode := legacyViewDefinitionSQLMode
	if data.SQLMode != nil {
		parserSQLMode = *data.SQLMode
	}
	statements, err := parsers.ParseWithSQLMode(
		ctx, dialect.MYSQL, data.Stmt, lowerCaseTableNames, parserSQLMode)
	if err != nil || len(statements) != 1 {
		return "", false
	}
	defer statements[0].Free()

	var selectStmt *tree.Select
	switch statement := statements[0].(type) {
	case *tree.CreateView:
		selectStmt = statement.AsSource
	case *tree.AlterView:
		selectStmt = statement.AsSource
	default:
		return "", false
	}
	if selectStmt == nil {
		return "", false
	}
	return tree.StringWithOpts(
		selectStmt, dialect.MYSQL, tree.WithQuoteString(true),
		tree.WithQuoteIdentifier(), tree.WithModeIndependentStringLiterals()), true
}
