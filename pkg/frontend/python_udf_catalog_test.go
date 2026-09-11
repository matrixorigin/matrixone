// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/udf"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/stretchr/testify/require"
)

func TestPersistPythonReplaceRejectsLegacyHeadBeforeWriting(t *testing.T) {
	ctx := context.Background()
	functionID := int64(41)
	languageSQL := fmt.Sprintf(
		"select language from mo_catalog.mo_user_defined_function where function_id = %d;",
		functionID,
	)
	headSQL := fmt.Sprintf(
		"select active_revision, namespace_version from mo_catalog.mo_user_defined_function where function_id = %d;",
		functionID,
	)
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	language := &MysqlResultSet{}
	languageColumn := &MysqlColumn{}
	languageColumn.SetName("language")
	languageColumn.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	language.AddColumn(languageColumn)
	language.AddRow([]interface{}{"python"})
	bh.sql2result[languageSQL] = language
	head := &MysqlResultSet{}
	for _, name := range []string{"active_revision", "namespace_version"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		head.AddColumn(column)
	}
	head.AddRow([]interface{}{int64(0), int64(0)})
	bh.sql2result[headSQL] = head

	err := persistUserDefinedFunction(ctx, bh, nil, 7, userDefinedFunctionDefinition{
		name:     "legacy_python",
		args:     "[]",
		argTypes: "[]",
		retType:  "int",
		body:     "legacy demo body",
		lang:     "python",
		dbName:   "db1",
	}, &functionID)

	require.ErrorContains(t, err, "UNSUPPORTED_ROUTINE_VERSION")
	require.ErrorContains(t, err, "DROP and CREATE")
	require.Equal(t, []string{languageSQL, headSQL}, bh.executedSqls)
}

func TestPersistFunctionReplaceRejectsLanguageChangeBeforeWriting(t *testing.T) {
	ctx := context.Background()
	functionID := int64(42)
	languageSQL := fmt.Sprintf(
		"select language from mo_catalog.mo_user_defined_function where function_id = %d;",
		functionID,
	)
	b := &backgroundExecTestWithHistory{}
	b.init()
	language := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName("language")
	column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	language.AddColumn(column)
	language.AddRow([]interface{}{"SQL"})
	b.sql2result[languageSQL] = language

	err := persistUserDefinedFunction(ctx, b, nil, 7, userDefinedFunctionDefinition{
		name: "cross_language",
		lang: "python",
		body: "not published",
	}, &functionID)

	require.ErrorContains(t, err, "cannot change language")
	require.ErrorContains(t, err, "DROP and CREATE")
	require.Equal(t, []string{languageSQL}, b.executedSqls)
}

func TestPersistPythonReplaceRejectsSignatureChangeBeforeWriting(t *testing.T) {
	ctx := context.Background()
	functionID := int64(43)
	languageSQL := fmt.Sprintf(
		"select language from mo_catalog.mo_user_defined_function where function_id = %d;",
		functionID,
	)
	headSQL := fmt.Sprintf(
		"select active_revision, namespace_version from mo_catalog.mo_user_defined_function where function_id = %d;",
		functionID,
	)
	bodySQL := fmt.Sprintf(
		"select body from mo_catalog.mo_function_revisions where function_id = %d and revision = %d and namespace_version = %d;",
		functionID, 1, 1,
	)
	b := &backgroundExecTestWithHistory{}
	b.init()
	b.sql2result[languageSQL] = singleStringResult("language", "python")
	head := &MysqlResultSet{}
	for _, name := range []string{"active_revision", "namespace_version"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		head.AddColumn(column)
	}
	head.AddRow([]interface{}{int64(1), int64(1)})
	b.sql2result[headSQL] = head
	previous := pythonCatalogTestBody(t, types.T_int64)
	b.sql2result[bodySQL] = singleStringResult("body", previous)
	candidate := pythonCatalogTestBody(t, types.T_int32)

	err := persistUserDefinedFunction(ctx, b, nil, 7, userDefinedFunctionDefinition{
		name:     "current_python",
		args:     "[]",
		argTypes: "[]",
		retType:  "int",
		body:     candidate,
		lang:     "python",
		dbName:   "db1",
	}, &functionID)

	require.ErrorContains(t, err, "cannot change input or return descriptor")
	require.Equal(t, []string{languageSQL, headSQL, bodySQL}, b.executedSqls)
}

func singleStringResult(name, value string) *MysqlResultSet {
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName(name)
	column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	result.AddColumn(column)
	result.AddRow([]interface{}{value})
	return result
}

func singleInt64Result(name string, value int64) *MysqlResultSet {
	result := &MysqlResultSet{}
	column := &MysqlColumn{}
	column.SetName(name)
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	result.AddColumn(column)
	result.AddRow([]interface{}{value})
	return result
}

func pythonCatalogTestBody(t *testing.T, oid types.T) string {
	t.Helper()
	descriptor, err := function.NewPythonTypeDescriptor(oid.ToType())
	require.NoError(t, err)
	source := "def f(ctx, value): return value"
	environment, err := udf.PythonEnvironmentDigest()
	require.NoError(t, err)
	body, err := json.Marshal(function.PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "f",
		Source:                  source,
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		ArtifactDigest:          udf.PythonInlineArtifactDigest("f", source),
		EnvironmentDigest:       environment,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []function.PythonTypeDescriptor{descriptor},
		ReturnType:              &descriptor,
	})
	require.NoError(t, err)
	return string(body)
}

func TestPersistPythonCreateRejectsMalformedDefinitionBeforeWriting(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	err := persistUserDefinedFunction(context.Background(), bh, &TenantInfo{User: "owner"}, 7, userDefinedFunctionDefinition{
		name:     "broken_python",
		args:     "[]",
		argTypes: "[]",
		retType:  "int",
		body:     "not current python routine json",
		lang:     "python",
		dbName:   "db1",
	}, nil)

	require.ErrorContains(t, err, "UNSUPPORTED_ROUTINE_VERSION")
	require.Empty(t, bh.executedSqls)
}

func TestPersistPythonCreateIdentityLookupIncludesLanguage(t *testing.T) {
	definition := userDefinedFunctionDefinition{
		name:                     "python_identity",
		dbName:                   "db1",
		canonicalInputDescriptor: "{\"types\":[]}",
		lang:                     "python",
	}
	query := fmt.Sprintf(
		`select function_id from mo_catalog.mo_user_defined_function where name = "%s" and db = "%s" and canonical_input_descriptor = "%s" and language = '%s' order by function_id desc limit 1;`,
		escapeSQLStringForDoubleQuotes(definition.name),
		escapeSQLStringForDoubleQuotes(definition.dbName),
		escapeSQLStringForDoubleQuotes(definition.canonicalInputDescriptor),
		escapeSQLStringForDoubleQuotes(definition.lang),
	)
	b := &backgroundExecTest{}
	b.init()
	b.sql2result[query] = singleInt64Result("function_id", 101)

	id, err := findPersistedFunctionID(context.Background(), b, definition)
	require.NoError(t, err)
	require.Equal(t, int64(101), id)
	require.Equal(t, []string{query}, b.executedSQLs)
	require.Contains(t, b.executedSQLs[0], "language = 'python'")
}

func TestPersistSQLCreateKeepsRollingCatalogCompatibility(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	bh.sql2err[functionRevisionCatalogSchemaCheck] = moerr.NewNoSuchTableNoCtx(
		"mo_catalog", "mo_function_revisions")

	err := persistUserDefinedFunction(context.Background(), bh, &TenantInfo{User: "owner"}, 7, userDefinedFunctionDefinition{
		name:     "sql_function",
		args:     `[{"name":"value","type":"bigint"}]`,
		argTypes: `["bigint"]`,
		retType:  "bigint",
		body:     "select value + 1",
		lang:     string(tree.SQL),
		dbName:   "db1",
	}, nil)
	require.NoError(t, err)
	require.Len(t, bh.executedSqls, 2)
	require.Equal(t, functionRevisionCatalogSchemaCheck, bh.executedSqls[0])
	require.NotContains(t, bh.executedSqls[1], "canonical_input_descriptor")
	require.NotContains(t, bh.executedSqls[1], "mo_function_revisions")
}

func TestFunctionRevisionCatalogAvailablePropagatesMalformedProbeResult(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()

	available, err := functionRevisionCatalogAvailable(context.Background(), bh)

	require.ErrorContains(t, err, "it is not the type of result set")
	require.False(t, available)
}

func TestFunctionRevisionCatalogAvailableRejectsSplitOrEmptyProbe(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []interface{}
	}{
		{name: "empty", results: []interface{}{}},
		{name: "split", results: []interface{}{emptyCatalogProbeResult(20), emptyCatalogProbeResult(20)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTestWithResults{results: tc.results}
			bh.init()
			available, err := functionRevisionCatalogAvailable(context.Background(), bh)
			require.ErrorContains(t, err, "expected exactly one")
			require.False(t, available)
		})
	}
}

type backgroundExecTestWithResults struct {
	backgroundExecTest
	results []interface{}
}

func (bt *backgroundExecTestWithResults) GetExecResultSet() []interface{} {
	return bt.results
}

func TestExactlyOneCatalogRowRejectsDuplicateOrSplitPointReads(t *testing.T) {
	one := singleStringResult("value", "current")
	row, err := exactlyOneCatalogRow(context.Background(), []ExecResult{one}, "routine point read")
	require.NoError(t, err)
	require.Same(t, one, row)

	one.AddRow([]interface{}{"duplicate"})
	_, err = exactlyOneCatalogRow(context.Background(), []ExecResult{one}, "routine point read")
	require.ErrorContains(t, err, "expected exactly one")

	_, err = exactlyOneCatalogRow(context.Background(), []ExecResult{singleStringResult("value", "one"), singleStringResult("value", "two")}, "routine point read")
	require.ErrorContains(t, err, "result sets")
}

func TestPersistSQLCreatePublishesSharedRevisionWhenCatalogIsReady(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	bh.sql2result[functionRevisionCatalogSchemaCheck] = emptyCatalogProbeResult(20)
	definition := userDefinedFunctionDefinition{
		name:     "sql_revision",
		args:     `[{"name":"value","type":"bigint"}]`,
		argTypes: `["bigint"]`,
		retType:  "bigint",
		body:     "select value + 1",
		lang:     string(tree.SQL),
		dbName:   "db1",
	}
	findSQL := `select function_id from mo_catalog.mo_user_defined_function where name = "sql_revision" and db = "db1" and arg_types = '["bigint"]' and language = 'sql' order by function_id desc limit 1;`
	bh.sql2result[findSQL] = singleInt64Result("function_id", 91)

	err := persistUserDefinedFunction(context.Background(), bh, &TenantInfo{User: "owner"}, 7, definition, nil)
	require.NoError(t, err)
	require.Len(t, bh.executedSqls, 5)
	require.Equal(t, functionRevisionCatalogSchemaCheck, bh.executedSqls[0])
	require.Contains(t, bh.executedSqls[1], "insert into mo_catalog.mo_user_defined_function")
	require.Equal(t, findSQL, bh.executedSqls[2])
	require.Contains(t, bh.executedSqls[3], "insert into mo_catalog.mo_function_revisions")
	require.Contains(t, bh.executedSqls[3], `"sql_revision"`)
	require.Contains(t, bh.executedSqls[3], `"select value + 1"`)
	require.Contains(t, bh.executedSqls[4], "active_revision = 1, namespace_version = 1")
}

func TestPersistPythonCreatePublishesInvokerSecurityType(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	body := pythonCatalogTestBody(t, types.T_int64)
	decoded, err := function.DecodePythonRoutineBody(body)
	require.NoError(t, err)
	canonicalInput, _, _, err := function.PythonSignatureMetadata(decoded.ArgTypes, decoded.ReturnType)
	require.NoError(t, err)
	definition := userDefinedFunctionDefinition{
		name:                     "python_invoker",
		args:                     `[{"name":"value","type":"bigint"}]`,
		argTypes:                 `["bigint"]`,
		retType:                  "bigint",
		body:                     body,
		lang:                     string(tree.PYTHON),
		dbName:                   "db1",
		canonicalInputDescriptor: canonicalInput,
	}
	findSQL := fmt.Sprintf(
		`select function_id from mo_catalog.mo_user_defined_function where name = "%s" and db = "%s" and canonical_input_descriptor = "%s" and language = '%s' order by function_id desc limit 1;`,
		escapeSQLStringForDoubleQuotes(definition.name),
		escapeSQLStringForDoubleQuotes(definition.dbName),
		escapeSQLStringForDoubleQuotes(canonicalInput),
		escapeSQLStringForDoubleQuotes(definition.lang),
	)
	bh.sql2result[findSQL] = singleInt64Result("function_id", 94)

	err = persistUserDefinedFunction(context.Background(), bh, &TenantInfo{User: "owner"}, 7, definition, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(bh.executedSqls), 1)
	require.Contains(t, bh.executedSqls[0], `"FUNCTION","INVOKER"`)
	for _, sql := range bh.executedSqls {
		if strings.Contains(sql, "mo_function_revisions") {
			require.Contains(t, sql, `"python_invoker"`)
		}
	}
}

func TestPublishRestoredPythonArtifactsRepublishesExactAccountArtifact(t *testing.T) {
	const (
		sid           = "python-artifact-restore-test"
		targetAccount = uint32(27)
	)
	InitServerLevelVars(sid)
	oldPU := getPuIfPresent(sid)
	t.Cleanup(func() { setPu(sid, oldPU) })

	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	setPu(sid, &config.ParameterUnit{FileService: fs})

	body := pythonCatalogTestBody(t, types.T_int64)
	decoded, err := function.DecodePythonRoutineBody(body)
	require.NoError(t, err)
	const query = "select language, artifact_digest, body from mo_catalog.mo_function_revisions order by function_id, revision;"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = newMrsForRestoreStringRows(
		[]string{"language", "artifact_digest", "body"},
		[][]interface{}{{"python", decoded.ArtifactDigest, body}},
	)

	err = publishRestoredPythonArtifacts(
		t.Context(), sid,
		bh,
		defines.AttachAccountId(t.Context(), targetAccount), targetAccount,
	)
	require.NoError(t, err)

	store, err := pythonudf.NewFileArtifactStore(fs, pythonudf.DefaultMaxArtifactBytes)
	require.NoError(t, err)
	resolved, err := store.Resolve(t.Context(), uint64(targetAccount), decoded.Handler, decoded.ArtifactDigest)
	require.NoError(t, err)
	require.Equal(t, decoded.Source, resolved)
	_, err = store.Resolve(t.Context(), uint64(targetAccount-1), decoded.Handler, decoded.ArtifactDigest)
	require.ErrorContains(t, err, "unavailable")
}

func TestPublishRestoredPythonArtifactsRejectsMissingArtifactStore(t *testing.T) {
	const sid = "python-artifact-restore-missing-store-test"
	InitServerLevelVars(sid)
	oldPU := getPuIfPresent(sid)
	t.Cleanup(func() { setPu(sid, oldPU) })
	setPu(sid, &config.ParameterUnit{})

	body := pythonCatalogTestBody(t, types.T_int64)
	decoded, err := function.DecodePythonRoutineBody(body)
	require.NoError(t, err)
	const query = "select language, artifact_digest, body from mo_catalog.mo_function_revisions order by function_id, revision;"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = newMrsForRestoreStringRows(
		[]string{"language", "artifact_digest", "body"},
		[][]interface{}{{"python", decoded.ArtifactDigest, body}},
	)

	err = publishRestoredPythonArtifacts(
		t.Context(), sid, bh,
		defines.AttachAccountId(t.Context(), 27), 27,
	)
	require.ErrorContains(t, err, "artifact store is required")
}

func TestPersistSQLReplacePublishesNextImmutableRevision(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	bh.sql2result[functionRevisionCatalogSchemaCheck] = emptyCatalogProbeResult(20)
	functionID := int64(92)
	bh.sql2result[fmt.Sprintf(
		"select language from mo_catalog.mo_user_defined_function where function_id = %d;", functionID,
	)] = singleStringResult("language", string(tree.SQL))
	bh.sql2result[fmt.Sprintf(
		"select active_revision, namespace_version from mo_catalog.mo_user_defined_function where function_id = %d;", functionID,
	)] = func() *MysqlResultSet {
		result := &MysqlResultSet{}
		for _, name := range []string{"active_revision", "namespace_version"} {
			column := &MysqlColumn{}
			column.SetName(name)
			column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{int64(2), int64(5)})
		return result
	}()
	bh.sql2result[fmt.Sprintf(
		"select rettype from mo_catalog.mo_user_defined_function where function_id = %d;", functionID,
	)] = singleStringResult("rettype", "bigint")

	err := persistUserDefinedFunction(context.Background(), bh, &TenantInfo{User: "owner"}, 7, userDefinedFunctionDefinition{
		name:     "sql_revision",
		args:     `[{"name":"value","type":"bigint"}]`,
		argTypes: `["bigint"]`,
		retType:  "bigint",
		body:     "select value + 2",
		lang:     string(tree.SQL),
		dbName:   "db1",
	}, &functionID)
	require.NoError(t, err)
	require.Len(t, bh.executedSqls, 7)
	require.Equal(t, functionRevisionCatalogSchemaCheck, bh.executedSqls[0])
	require.Contains(t, bh.executedSqls[4], "body = \"select value + 2\"")
	require.Contains(t, bh.executedSqls[5], "function_id, revision, namespace_version")
	require.Contains(t, bh.executedSqls[5], ", 3, 6,")
	require.Contains(t, bh.executedSqls[6], "active_revision = 3, namespace_version = 6")
}

func TestReadSQLRevisionBindsActiveHeadAndRejectsTampering(t *testing.T) {
	const functionID = int64(93)
	const args = `[{"name":"value","type":"bigint"}]`
	const argTypes = `["bigint"]`
	const body = "select value + 1"
	const retType = "bigint"
	fingerprint, err := function.SQLRoutineFingerprint(body, argTypes, retType)
	require.NoError(t, err)
	baseSQL := fmt.Sprintf("select active_revision, namespace_version, security_type from mo_catalog.mo_user_defined_function where function_id = %d;", functionID)
	revisionSQL := fmt.Sprintf("select revision, args, arg_types, body, rettype, language, definition_schema_version, definition_fingerprint, volatility, null_policy, security_type from mo_catalog.mo_function_revisions where function_id = %d and revision = 1 and namespace_version = 1;", functionID)
	newBase := func() *MysqlResultSet {
		result := &MysqlResultSet{}
		for _, name := range []string{"active_revision", "namespace_version", "security_type"} {
			column := &MysqlColumn{}
			column.SetName(name)
			if name == "security_type" {
				column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			} else {
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
			}
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{int64(1), int64(1), "DEFINER"})
		return result
	}
	newRevision := func(candidateBody, candidateFingerprint string) *MysqlResultSet {
		result := &MysqlResultSet{}
		for _, name := range []string{"revision", "args", "arg_types", "body", "rettype", "language", "definition_schema_version", "definition_fingerprint", "volatility", "null_policy", "security_type"} {
			column := &MysqlColumn{}
			column.SetName(name)
			if name == "revision" || name == "definition_schema_version" {
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
			} else {
				column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			}
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{int64(1), args, argTypes, candidateBody, retType, string(tree.SQL), int64(udf.SQLDefinitionSchemaVersion), candidateFingerprint, "VOLATILE", udf.NullCallHandler, "DEFINER"})
		return result
	}

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[baseSQL] = newBase()
	bh.sql2result[revisionSQL] = newRevision(body, fingerprint)
	got, found, err := readSQLRevision(context.Background(), bh, functionID, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), got.Revision)
	require.Equal(t, body, got.Body)
	require.Equal(t, fingerprint, got.DefinitionFingerprint)

	bh.init()
	bh.sql2result[baseSQL] = newBase()
	bh.sql2result[revisionSQL] = newRevision(body, strings.Repeat("a", 64))
	_, found, err = readSQLRevision(context.Background(), bh, functionID, nil)
	require.ErrorContains(t, err, "fingerprint mismatch")
	require.False(t, found)
}

func TestEnsurePythonUdfCatalogReadyRequiresCurrentSchema(t *testing.T) {
	bh := &backgroundExecTestWithHistory{}
	bh.init()
	bh.sql2result[pythonUdfCatalogIdentitySchemaCheck] = emptyCatalogProbeResult(6)
	bh.sql2result[functionRevisionCatalogSchemaCheck] = emptyCatalogProbeResult(20)
	require.NoError(t, ensurePythonUdfCatalogReady(context.Background(), bh))
	require.Equal(t, []string{
		pythonUdfCatalogIdentitySchemaCheck,
		functionRevisionCatalogSchemaCheck,
	}, bh.executedSqls)

	bh.init()
	bh.sql2result[pythonUdfCatalogIdentitySchemaCheck] = emptyCatalogProbeResult(6)
	wantErr := fmt.Errorf("missing current revision catalog")
	bh.sql2err[functionRevisionCatalogSchemaCheck] = wantErr
	err := ensurePythonUdfCatalogReady(context.Background(), bh)
	require.ErrorContains(t, err, "Python UDF catalog contract is not ready")
	require.ErrorContains(t, err, wantErr.Error())
	require.Equal(t, []string{
		pythonUdfCatalogIdentitySchemaCheck,
		functionRevisionCatalogSchemaCheck,
	}, bh.executedSqls)

	bh.init()
	bh.sql2result[pythonUdfCatalogIdentitySchemaCheck] = emptyCatalogProbeResult(1)
	bh.sql2result[functionRevisionCatalogSchemaCheck] = emptyCatalogProbeResult(20)
	err = ensurePythonUdfCatalogReady(context.Background(), bh)
	require.ErrorContains(t, err, "catalog probe returned 1 columns, expected 6")
}

func emptyCatalogProbeResult(columnCount int) *MysqlResultSet {
	result := &MysqlResultSet{}
	for index := 0; index < columnCount; index++ {
		column := &MysqlColumn{}
		column.SetName(fmt.Sprintf("column_%d", index))
		column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
		result.AddColumn(column)
	}
	values := make([]interface{}, columnCount)
	for index := range values {
		values[index] = int64(0)
	}
	result.AddRow(values)
	return result
}

func TestValidateRestoredPythonRevisionCatalogChecksCurrentContract(t *testing.T) {
	body := pythonCatalogTestBody(t, types.T_int64)
	fingerprint, err := function.PythonRoutineFingerprint(body)
	require.NoError(t, err)
	decodedBody, err := function.DecodePythonRoutineBody(body)
	require.NoError(t, err)
	inputDescriptor, _, _, err := function.PythonSignatureMetadata(decodedBody.ArgTypes, decodedBody.ReturnType)
	require.NoError(t, err)
	query := fmt.Sprintf(
		"select %s from %s order by function_id, revision;",
		functionRevisionCatalogColumns,
		qualifiedTableName(moCatalog, "mo_function_revisions"),
	)
	result := &MysqlResultSet{}
	for index := 0; index < 20; index++ {
		column := &MysqlColumn{}
		column.SetName(fmt.Sprintf("column_%d", index))
		column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{
		"44", "1", "1", "f", `[ {"name":"value","type":"bigint"} ]`, inputDescriptor, "bigint", body,
		"python", "1", udf.PythonABIContract, udf.PythonAdapterVersion,
		udf.PythonInlineArtifactDigest("f", "def f(ctx, value): return value"),
		func() string {
			value, digestErr := udf.PythonEnvironmentDigest()
			require.NoError(t, digestErr)
			return value
		}(),
		udf.PythonSDKVersion, udf.NullCallHandler, "VOLATILE", fingerprint, "2026-09-11 00:00:00", "INVOKER",
	})

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = result
	require.NoError(t, validateRestoredFunctionRevisionCatalog(context.Background(), bh, context.Background()))

	result.Data[0][17] = "stale-fingerprint"
	bh.init()
	bh.sql2result[query] = result
	require.ErrorContains(t,
		validateRestoredFunctionRevisionCatalog(context.Background(), bh, context.Background()),
		"fingerprint mismatch",
	)
}

func TestValidateRestoredSharedRevisionCatalogAcceptsSQLAndRejectsTampering(t *testing.T) {
	args := `[ {"name":"value","type":"bigint"} ]`
	argTypes, err := userDefinedFunctionArgumentTypesFromJSON(args)
	require.NoError(t, err)
	body := "select value + 1"
	retType := "bigint"
	fingerprint, err := function.SQLRoutineFingerprint(body, argTypes, retType)
	require.NoError(t, err)
	query := fmt.Sprintf(
		"select %s from %s order by function_id, revision;",
		functionRevisionCatalogColumns,
		qualifiedTableName(moCatalog, "mo_function_revisions"),
	)
	newResult := func(body, digest string) *MysqlResultSet {
		result := &MysqlResultSet{}
		for index := 0; index < 20; index++ {
			column := &MysqlColumn{}
			column.SetName(fmt.Sprintf("column_%d", index))
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{
			"45", "1", "1", "f", args, argTypes, retType, body,
			string(tree.SQL), strconv.Itoa(udf.SQLDefinitionSchemaVersion), "", "", "", "", "",
			udf.NullCallHandler, "VOLATILE", digest, "2026-09-11 00:00:00", "DEFINER",
		})
		return result
	}

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = newResult(body, fingerprint)
	require.NoError(t, validateRestoredFunctionRevisionCatalog(context.Background(), bh, context.Background()))

	bh.init()
	bh.sql2result[query] = newResult("select value + 2", fingerprint)
	require.ErrorContains(t,
		validateRestoredFunctionRevisionCatalog(context.Background(), bh, context.Background()),
		"fingerprint mismatch",
	)
}

func TestValidateRestoredPythonCatalogHeadsBindsIdentityToActiveRevision(t *testing.T) {
	body := pythonCatalogTestBody(t, types.T_int64)
	decoded, err := function.DecodePythonRoutineBody(body)
	require.NoError(t, err)
	input, output, signature, err := function.PythonSignatureMetadata(decoded.ArgTypes, decoded.ReturnType)
	require.NoError(t, err)
	fingerprint, err := function.PythonRoutineFingerprint(body)
	require.NoError(t, err)
	query := `select cast(f.function_id as char), cast(f.active_revision as char), cast(f.namespace_version as char),
		cast(coalesce(r.revision, 0) as char), cast(coalesce(r.namespace_version, 0) as char),
		f.canonical_input_descriptor, f.return_descriptor,
		cast(f.signature_key_schema_version as char), f.signature_fingerprint,
		coalesce(r.arg_types, ''), coalesce(r.rettype, ''), coalesce(r.body, ''),
		lower(f.language), coalesce(r.definition_fingerprint, ''),
		coalesce(r.security_type, ''), coalesce(f.security_type, '')
		from mo_catalog.mo_user_defined_function f
		left join mo_catalog.mo_function_revisions r
			on r.function_id = f.function_id and r.revision = f.active_revision
			and r.namespace_version = f.namespace_version
		where lower(f.language) in ("python", "sql")
		order by f.function_id;`
	newResult := func() *MysqlResultSet {
		result := &MysqlResultSet{}
		for index := 0; index < 16; index++ {
			column := &MysqlColumn{}
			column.SetName(fmt.Sprintf("column_%d", index))
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{
			"44", "2", "3", "2", "3", input, output,
			fmt.Sprintf("%d", udf.PythonSignatureKeySchemaVersion), signature,
			input, "bigint", body, string(tree.PYTHON), fingerprint,
			"INVOKER", "INVOKER",
		})
		return result
	}

	b := &backgroundExecTest{}
	b.init()
	b.sql2result[query] = newResult()
	require.NoError(t, validateRestoredFunctionCatalogHeads(context.Background(), b, context.Background()))

	mutated := newResult()
	mutated.Data[0][1] = "1"
	b.init()
	b.sql2result[query] = mutated
	require.ErrorContains(t,
		validateRestoredFunctionCatalogHeads(context.Background(), b, context.Background()),
		"active revision does not exist",
	)

	mutated = newResult()
	mutated.Data[0][8] = "stale-signature"
	b.init()
	b.sql2result[query] = mutated
	require.ErrorContains(t,
		validateRestoredFunctionCatalogHeads(context.Background(), b, context.Background()),
		"head identity does not match",
	)
}

func TestValidateRestoredSharedCatalogHeadsChecksSQLRevision(t *testing.T) {
	args := `[ {"name":"value","type":"bigint"} ]`
	argTypes, err := userDefinedFunctionArgumentTypesFromJSON(args)
	require.NoError(t, err)
	body := "select value + 1"
	retType := "bigint"
	fingerprint, err := function.SQLRoutineFingerprint(body, argTypes, retType)
	require.NoError(t, err)
	query := `select cast(f.function_id as char), cast(f.active_revision as char), cast(f.namespace_version as char),
		cast(coalesce(r.revision, 0) as char), cast(coalesce(r.namespace_version, 0) as char),
		f.canonical_input_descriptor, f.return_descriptor,
		cast(f.signature_key_schema_version as char), f.signature_fingerprint,
		coalesce(r.arg_types, ''), coalesce(r.rettype, ''), coalesce(r.body, ''),
		lower(f.language), coalesce(r.definition_fingerprint, ''),
		coalesce(r.security_type, ''), coalesce(f.security_type, '')
		from mo_catalog.mo_user_defined_function f
		left join mo_catalog.mo_function_revisions r
			on r.function_id = f.function_id and r.revision = f.active_revision
			and r.namespace_version = f.namespace_version
		where lower(f.language) in ("python", "sql")
		order by f.function_id;`
	newResult := func(body, digest string) *MysqlResultSet {
		result := &MysqlResultSet{}
		for index := 0; index < 16; index++ {
			column := &MysqlColumn{}
			column.SetName(fmt.Sprintf("column_%d", index))
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			result.AddColumn(column)
		}
		result.AddRow([]interface{}{
			"45", "1", "1", "1", "1", "", "", "0", "",
			argTypes, retType, body, string(tree.SQL), digest,
			"DEFINER", "DEFINER",
		})
		return result
	}

	b := &backgroundExecTest{}
	b.init()
	b.sql2result[query] = newResult(body, fingerprint)
	require.NoError(t, validateRestoredFunctionCatalogHeads(context.Background(), b, context.Background()))

	b.init()
	b.sql2result[query] = newResult("select value + 2", fingerprint)
	require.ErrorContains(t,
		validateRestoredFunctionCatalogHeads(context.Background(), b, context.Background()),
		"head fingerprint does not match",
	)
}

func TestReadPythonRevisionRequiresPersistedIdentitySignature(t *testing.T) {
	body := pythonCatalogTestBody(t, types.T_int64)
	inputDescriptor, returnDescriptor, signatureFingerprint, err := func() (string, string, string, error) {
		decoded, decodeErr := function.DecodePythonRoutineBody(body)
		if decodeErr != nil {
			return "", "", "", decodeErr
		}
		return function.PythonSignatureMetadata(decoded.ArgTypes, decoded.ReturnType)
	}()
	require.NoError(t, err)
	fingerprint, err := function.PythonRoutineFingerprint(body)
	require.NoError(t, err)
	environment, err := udf.PythonEnvironmentDigest()
	require.NoError(t, err)
	query := pythonRevisionCatalogSQL(nil, 44)
	result := &MysqlResultSet{}
	for index := 0; index < 23; index++ {
		column := &MysqlColumn{}
		column.SetName(fmt.Sprintf("column_%d", index))
		if index == 0 || index == 1 || index == 2 || index == 5 || index == 12 {
			column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		} else {
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
		}
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{
		int64(1), int64(1), int64(1), inputDescriptor, returnDescriptor, int64(udf.PythonSignatureKeySchemaVersion), signatureFingerprint,
		`[{"name":"value","type":"bigint"}]`, inputDescriptor, body, "python", "bigint", int64(udf.PythonDefinitionSchemaVersion),
		udf.PythonABIContract, udf.PythonAdapterVersion, udf.PythonSDKVersion, udf.NullCallHandler, "VOLATILE", fingerprint,
		udf.PythonInlineArtifactDigest("f", "def f(ctx, value): return value"), environment,
		"INVOKER", "INVOKER",
	})
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = result
	revision, err := readPythonRevision(context.Background(), bh, 44, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), revision.Revision)
	require.Equal(t, inputDescriptor, revision.CanonicalInputDescriptor)

	result.Data[0][3] = "[]"
	bh.init()
	bh.sql2result[query] = result
	_, err = readPythonRevision(context.Background(), bh, 44, nil)
	require.ErrorContains(t, err, "identity descriptor metadata mismatch")

	result.Data[0][3] = inputDescriptor
	result.Data[0][8] = "[]"
	bh.init()
	bh.sql2result[query] = result
	_, err = readPythonRevision(context.Background(), bh, 44, nil)
	require.ErrorContains(t, err, "revision argument descriptor mismatch")

	result.Data[0][8] = inputDescriptor
	result.Data[0][21] = "DEFINER"
	bh.init()
	bh.sql2result[query] = result
	_, err = readPythonRevision(context.Background(), bh, 44, nil)
	require.ErrorContains(t, err, "revision contract is not supported")
}

func TestMatchUserDefinedFunctionCandidatesUsesExactPythonDescriptor(t *testing.T) {
	decimal2, err := function.NewPythonTypeDescriptor(types.New(types.T_decimal64, 18, 2))
	require.NoError(t, err)
	decimal6, err := function.NewPythonTypeDescriptor(types.New(types.T_decimal64, 18, 6))
	require.NoError(t, err)
	returnType, err := function.NewPythonTypeDescriptor(types.T_decimal64.ToType())
	require.NoError(t, err)
	exact2, err := canonicalPythonInputDescriptor([]function.PythonTypeDescriptor{decimal2}, &returnType)
	require.NoError(t, err)
	exact6, err := canonicalPythonInputDescriptor([]function.PythonTypeDescriptor{decimal6}, &returnType)
	require.NoError(t, err)

	result := &MysqlResultSet{}
	for _, name := range []string{"function_id", "args", "language", "arg_types", "canonical_input_descriptor"} {
		column := &MysqlColumn{}
		column.SetName(name)
		if name == "function_id" {
			column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		} else {
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
		}
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{int64(51), `[{"name":"value","type":"decimal"}]`, "python", exact2, exact2})
	result.AddRow([]interface{}{int64(52), `[{"name":"value","type":"decimal"}]`, "python", exact6, exact6})
	result.AddRow([]interface{}{int64(53), `[{"name":"value","type":"decimal"}]`, "sql", `["decimal"]`, ""})

	ids, err := matchUserDefinedFunctionCandidates(context.Background(), []ExecResult{result}, "python", `["decimal"]`, exact2)
	require.NoError(t, err)
	// A SQL routine with the same logical signature still occupies the shared
	// namespace. The exact descriptor distinguishes the two Python overloads;
	// it does not permit an ambiguous SQL/Python pair.
	require.Equal(t, []int64{51, 53}, ids)

	ids, err = matchUserDefinedFunctionCandidates(context.Background(), []ExecResult{result}, "sql", `["decimal"]`, `["decimal"]`)
	require.NoError(t, err)
	require.Equal(t, []int64{51, 52, 53}, ids)
}

func TestMatchUserDefinedFunctionCandidatesRejectsSplitResultSets(t *testing.T) {
	result := emptyCatalogProbeResult(5)
	_, err := matchUserDefinedFunctionCandidates(
		context.Background(),
		[]ExecResult{result, result},
		string(tree.PYTHON), `[]`, `[]`,
	)
	require.ErrorContains(t, err, "function catalog candidate lookup returned 2 result sets")
}

func TestMatchUserDefinedFunctionCandidatesAcceptsEmptyLookup(t *testing.T) {
	ids, err := matchUserDefinedFunctionCandidates(
		context.Background(), nil,
		string(tree.PYTHON), `[]`, `[]`,
	)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestPythonDropSignatureUsesExactDescriptor(t *testing.T) {
	statement, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "drop function db.f(decimal(18,2))", 1)
	require.NoError(t, err)
	defer statement.Free()
	drop, ok := statement.(*tree.DropFunction)
	require.True(t, ok)

	decimal2, err := function.NewPythonTypeDescriptor(types.New(types.T_decimal128, 18, 2))
	require.NoError(t, err)
	decimal6, err := function.NewPythonTypeDescriptor(types.New(types.T_decimal128, 18, 6))
	require.NoError(t, err)
	returnType, err := function.NewPythonTypeDescriptor(types.T_decimal64.ToType())
	require.NoError(t, err)
	makeBody := func(argument function.PythonTypeDescriptor) string {
		source := "def f(ctx, value): return value"
		environment, environmentErr := udf.PythonEnvironmentDigest()
		require.NoError(t, environmentErr)
		body, bodyErr := json.Marshal(function.PythonRoutineBody{
			DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
			Handler:                 "f",
			Source:                  source,
			Mode:                    "SCALAR",
			NullPolicy:              udf.NullCallHandler,
			ABIContract:             udf.PythonABIContract,
			AdapterVersion:          udf.PythonAdapterVersion,
			ArtifactDigest:          udf.PythonInlineArtifactDigest("f", source),
			EnvironmentDigest:       environment,
			SDKVersion:              udf.PythonSDKVersion,
			ArgTypes:                []function.PythonTypeDescriptor{argument},
			ReturnType:              &returnType,
		})
		require.NoError(t, bodyErr)
		return string(body)
	}

	require.True(t, pythonDropSignatureMatches(drop.Args, makeBody(decimal2)))
	require.False(t, pythonDropSignatureMatches(drop.Args, makeBody(decimal6)))
}
