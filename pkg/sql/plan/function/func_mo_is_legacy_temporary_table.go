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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const temporaryTableSessionIDLength = 32

func builtInIsLegacyTemporaryTable(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	relKinds := vector.GenerateFunctionStrParameter(parameters[0])
	relNames := vector.GenerateFunctionStrParameter(parameters[1])
	databases := vector.GenerateFunctionStrParameter(parameters[2])
	createSQLs := vector.GenerateFunctionStrParameter(parameters[3])
	extraInfos := vector.GenerateFunctionStrParameter(parameters[4])
	results := vector.MustFunctionResult[bool](result)

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(row) {
			if err := results.Append(false, true); err != nil {
				return err
			}
			continue
		}

		relKind, nullKind := relKinds.GetStrValue(row)
		relName, nullName := relNames.GetStrValue(row)
		database, nullDatabase := databases.GetStrValue(row)
		createSQL, nullCreateSQL := createSQLs.GetStrValue(row)
		extraInfo, nullExtraInfo := extraInfos.GetStrValue(row)
		if nullKind || nullName || nullDatabase || nullCreateSQL || nullExtraInfo {
			if err := results.Append(false, true); err != nil {
				return err
			}
			continue
		}

		isLegacy := isLegacyTemporaryTable(
			proc.Ctx,
			functionUtil.QuickBytesToStr(relKind),
			functionUtil.QuickBytesToStr(relName),
			functionUtil.QuickBytesToStr(database),
			functionUtil.QuickBytesToStr(createSQL),
			functionUtil.QuickBytesToStr(extraInfo),
		)
		if err := results.Append(isLegacy, false); err != nil {
			return err
		}
	}
	return nil
}

func isLegacyTemporaryTable(ctx context.Context, relKind, relName, database, createSQL, extraInfo string) bool {
	if relKind != catalog.SystemOrdinaryRel || createSQL == "" {
		return false
	}
	logicalName, ok := legacyTemporaryTableLogicalName(relName, database)
	if !ok {
		return false
	}

	// Temporary tables could not be renamed, while rename preserves OldName in
	// SchemaExtra. Treat malformed metadata as permanent as well: compatibility
	// classification must fail open rather than hide a user table.
	if extraInfo != "" {
		extra := &api.SchemaExtra{}
		if err := extra.Unmarshal([]byte(extraInfo)); err != nil || extra.OldName != "" {
			return false
		}
	}

	statements, err := parsers.Parse(ctx, dialect.MYSQL, createSQL, 1)
	if err != nil {
		return false
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()

	temporaryStatementMatches := false
	for _, statement := range statements {
		createTable, ok := statement.(*tree.CreateTable)
		if !ok {
			continue
		}

		// Legacy rel_createsql may contain the entire COM_QUERY. A permanent
		// table's physical-looking name is explicit in its own CREATE statement,
		// whereas a temporary table's generated physical name never is. This
		// durable evidence must win over a matching temporary alias elsewhere in
		// the same request.
		if !createTable.Temporary && createTableNamesRelation(createTable, relName, database) {
			return false
		}
		if createTable.Temporary && createTableNamesRelation(createTable, logicalName, database) {
			temporaryStatementMatches = true
		}
	}
	return temporaryStatementMatches
}

func createTableNamesRelation(createTable *tree.CreateTable, name, database string) bool {
	if !strings.EqualFold(string(createTable.Table.ObjectName), name) {
		return false
	}
	schemaName := string(createTable.Table.SchemaName)
	return schemaName == "" || strings.EqualFold(schemaName, database)
}

func legacyTemporaryTableLogicalName(relName, database string) (string, bool) {
	if database == "" || !strings.HasPrefix(relName, defines.TempTableNamePrefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(relName, defines.TempTableNamePrefix)
	sessionID, physicalName, found := strings.Cut(remainder, "_")
	if !found || len(sessionID) != temporaryTableSessionIDLength || !isLowerHex(sessionID) {
		return "", false
	}
	databasePrefix := database + "_"
	if len(physicalName) <= len(databasePrefix) ||
		!strings.EqualFold(physicalName[:len(databasePrefix)], databasePrefix) {
		return "", false
	}
	return physicalName[len(databasePrefix):], true
}

func isLowerHex(value string) bool {
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
