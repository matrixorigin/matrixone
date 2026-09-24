// Copyright 2021 Matrix Origin
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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// RegeneratedViewDefinition is produced by exactly the same schema generator
// used for CREATE/ALTER VIEW. Callers must replace the complete column list;
// applying individual field patches would create a second inference path.
type RegeneratedViewDefinition struct {
	TableDef     *planpb.TableDef
	Dependencies []ViewDependency
}

// ReplaceRegeneratedViewDependencies replaces the dependency snapshot in an
// already regenerated definition while preserving every unrelated ViewData
// field. It is used when the catalog mutation boundary can provide a canonical
// logical identity that a same-transaction relation handle cannot yet expose.
func ReplaceRegeneratedViewDependencies(
	regenerated *RegeneratedViewDefinition,
	dependencies []ViewDependency,
) error {
	if regenerated == nil || regenerated.TableDef == nil || regenerated.TableDef.ViewSql == nil {
		return moerr.NewInternalErrorNoCtx("invalid regenerated View definition")
	}
	var data ViewData
	if err := json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &data); err != nil {
		return err
	}
	lowerCaseTableNames := int64(0)
	if data.LowerCaseTableNames != nil {
		lowerCaseTableNames = *data.LowerCaseTableNames
	}
	updated, err := patchPersistedViewMetadata(
		regenerated.TableDef.ViewSql.View, nil, dependencies,
		lowerCaseTableNames, data.RequiredProtocolVersion)
	if err != nil {
		return err
	}
	regenerated.TableDef.ViewSql.View = updated
	regenerated.Dependencies = dependencies
	return nil
}

type viewRegenerationContext struct {
	CompilerContext
	defaultDatabase     string
	rootSQL             string
	lowerCaseTableNames int64
}

// Use the error-returning catalog lookup rather than DatabaseExists: the
// latter collapses both genuine absence and storage failures into false.
func (c *viewRegenerationContext) CheckViewDatabase(name string, snapshot *Snapshot) (bool, error) {
	_, err := c.CompilerContext.GetDatabaseId(name, snapshot)
	if err == nil {
		return true, nil
	}
	if moerr.IsMoErrCode(err, moerr.ErrBadDB) || moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		return false, nil
	}
	return false, err
}

func (c *viewRegenerationContext) DefaultDatabase() string { return c.defaultDatabase }
func (c *viewRegenerationContext) GetRootSql() string      { return c.rootSQL }
func (c *viewRegenerationContext) GetLowerCaseTableNames() int64 {
	return c.lowerCaseTableNames
}

func (c *viewRegenerationContext) ResolveViewDependencyAccount(
	obj *ObjectRef,
	tableDef *TableDef,
	snapshot *Snapshot,
) (uint32, error) {
	if resolver, ok := c.CompilerContext.(ViewDependencyIdentityResolver); ok {
		return resolver.ResolveViewDependencyAccount(obj, tableDef, snapshot)
	}
	accountID, err := c.CompilerContext.GetAccountId()
	if err != nil {
		return 0, err
	}
	if obj.PubInfo != nil {
		accountID = uint32(obj.PubInfo.TenantId)
	} else if snapshot != nil && snapshot.Tenant != nil {
		accountID = snapshot.Tenant.TenantID
	}
	return accountID, nil
}

// RegenerateViewDefinition parses a persisted View with its original lexical
// and database context, then delegates to genViewTableDef. Unknown ViewData JSON
// fields are retained when the dependency snapshot is updated.
// DescribeViewColumns binds the persisted semantic definition in the caller's
// catalog context. It never writes the regenerated definition or dependencies.
func DescribeViewColumns(ctx CompilerContext, persistedViewData string) ([]*planpb.ColDef, error) {
	if len(persistedViewData) > 16<<20 {
		return nil, moerr.NewInternalError(ctx.GetContext(), "View definition exceeds metadata binding budget")
	}
	if err := ctx.GetContext().Err(); err != nil {
		return nil, err
	}
	regenerated, err := RegenerateViewDefinition(ctx, persistedViewData)
	if err != nil {
		return nil, err
	}
	return regenerated.TableDef.Cols, nil
}

func RegenerateViewDefinition(
	ctx CompilerContext,
	persistedViewData string,
) (*RegeneratedViewDefinition, error) {
	var viewData ViewData
	if err := json.Unmarshal([]byte(persistedViewData), &viewData); err != nil {
		return nil, err
	}
	if viewData.RequiredProtocolVersion != nil {
		if *viewData.RequiredProtocolVersion < 0 {
			return nil, moerr.NewInvalidInputf(
				ctx.GetContext(),
				"persisted view protocol version must not be negative: %d",
				*viewData.RequiredProtocolVersion)
		}
		if err := RequirePersistedProtocolVersion(
			ctx.GetContext(), ctx.GetProcess(), *viewData.RequiredProtocolVersion); err != nil {
			return nil, err
		}
	}
	parserSQLMode := legacyViewParserSQLMode
	if viewData.SQLMode != nil {
		parserSQLMode = *viewData.SQLMode
	}
	lowerCaseTableNames := ctx.GetLowerCaseTableNames()
	if viewData.LowerCaseTableNames != nil {
		lowerCaseTableNames = *viewData.LowerCaseTableNames
	}
	statements, err := parsers.ParseWithSQLMode(
		ctx.GetContext(), dialect.MYSQL, viewData.Stmt, lowerCaseTableNames, parserSQLMode)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()
	if len(statements) != 1 {
		return nil, moerr.NewParseError(ctx.GetContext(), "persisted View must contain one statement")
	}

	var selectStmt *tree.Select
	var columnNames tree.IdentifierList
	var viewDatabase, viewName string
	switch statement := statements[0].(type) {
	case *tree.CreateView:
		selectStmt, columnNames = statement.AsSource, statement.ColNames
		viewDatabase, viewName = string(statement.Name.SchemaName), string(statement.Name.ObjectName)
	case *tree.AlterView:
		selectStmt, columnNames = statement.AsSource, statement.ColNames
		viewDatabase, viewName = string(statement.Name.SchemaName), string(statement.Name.ObjectName)
	default:
		return nil, moerr.NewParseError(ctx.GetContext(), "persisted View statement is not CREATE/ALTER VIEW")
	}
	if viewDatabase == "" {
		viewDatabase = viewData.DefaultDatabase
	}

	regenerationCtx := &viewRegenerationContext{
		CompilerContext:     ctx,
		defaultDatabase:     viewData.DefaultDatabase,
		rootSQL:             viewData.Stmt,
		lowerCaseTableNames: lowerCaseTableNames,
	}
	tableDef, err := genViewTableDef(
		regenerationCtx, selectStmt, columnNames, viewDatabase, viewName, false)
	if err != nil {
		return nil, err
	}
	var generatedData ViewData
	if err = json.Unmarshal([]byte(tableDef.ViewSql.View), &generatedData); err != nil {
		return nil, err
	}

	updatedViewData, err := patchPersistedViewMetadata(
		persistedViewData, &generatedData.Stmt, generatedData.Dependencies,
		lowerCaseTableNames, maxPersistedProtocolVersion(
			viewData.RequiredProtocolVersion, generatedData.RequiredProtocolVersion))
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql.View = updatedViewData
	return &RegeneratedViewDefinition{
		TableDef:     tableDef,
		Dependencies: generatedData.Dependencies,
	}, nil
}

func patchPersistedViewMetadata(
	persistedViewData string,
	stableStatement *string,
	dependencies []ViewDependency,
	lowerCaseTableNames int64,
	requiredProtocolVersion *int64,
) (string, error) {
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal([]byte(persistedViewData), &fields); err != nil {
		return "", err
	}
	encodedDependencies, err := json.Marshal(dependencies)
	if err != nil {
		return "", err
	}
	if stableStatement != nil {
		encodedStatement, marshalErr := json.Marshal(*stableStatement)
		if marshalErr != nil {
			return "", marshalErr
		}
		fields["Stmt"] = encodedStatement
	}
	fields["dependencies"] = encodedDependencies
	if requiredProtocolVersion != nil {
		encodedRequiredProtocolVersion, marshalErr := json.Marshal(*requiredProtocolVersion)
		if marshalErr != nil {
			return "", marshalErr
		}
		fields["required_protocol_version"] = encodedRequiredProtocolVersion
	}
	if _, ok := fields["lower_case_table_names"]; !ok {
		encodedLowerCaseTableNames, marshalErr := json.Marshal(lowerCaseTableNames)
		if marshalErr != nil {
			return "", marshalErr
		}
		fields["lower_case_table_names"] = encodedLowerCaseTableNames
	}
	updated, err := json.Marshal(fields)
	return string(updated), err
}

func maxPersistedProtocolVersion(values ...*int64) *int64 {
	var max int64
	seen := false
	for _, value := range values {
		if value == nil || *value < 0 {
			continue
		}
		if !seen || *value > max {
			max = *value
			seen = true
		}
	}
	if !seen {
		return nil
	}
	return &max
}
