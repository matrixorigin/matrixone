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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/udf"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
)

type systemCatalogRestorePolicy uint8

const (
	// systemCatalogRestoreSkip means that DDL or another subsystem owns the
	// target state, so snapshot restore must not copy the historical table.
	systemCatalogRestoreSkip systemCatalogRestorePolicy = iota
	// systemCatalogRestoreCopy is safe only for catalog rows whose identifiers
	// retain their meaning in the target account.
	systemCatalogRestoreCopy
	// systemCatalogRestoreRebuild skips the generic table copy and lets the table
	// owner rebuild its rows after every referenced object is restored.
	systemCatalogRestoreRebuild
)

func (policy systemCatalogRestorePolicy) skipsBulkRestore() bool {
	return policy == systemCatalogRestoreSkip || policy == systemCatalogRestoreRebuild
}

type systemCatalogRestoreContext struct {
	ctx           context.Context
	sid           string
	bh            BackgroundExec
	snapshotTS    int64
	sourceAccount uint32
	targetAccount uint32
}

// catalogRestoreAccountPair preserves the source identity used for historical
// reads and the target identity used for reconstructed catalog rows.
type catalogRestoreAccountPair struct {
	sourceAccount uint32
	targetAccount uint32
}

type systemCatalogPostRestoreHandler struct {
	tableName string
	handler   func(*systemCatalogRestoreContext) error
}

// The slice is deliberately ordered. To add another identity-bearing catalog:
//
//  1. mark it systemCatalogRestoreRebuild in the policy table;
//  2. register its owner-specific handler here, after any catalog it consumes;
//  3. test both its ID semantics and every account-restore entry point.
//
// The runtime policy/handler cross-check below makes a half-registered catalog
// fail before any target catalog rows are rewritten.
var systemCatalogPostRestoreHandlers = []systemCatalogPostRestoreHandler{
	{tableName: "mo_role_privs", handler: restoreRolePrivilegesAfterObjects},
}

const rolePrivilegeRestoreInsertBatchSize = 256

const userDefinedFunctionCatalogColumns = `function_id, active_revision, namespace_version, name, owner, args, arg_types, canonical_input_descriptor, return_descriptor, signature_key_schema_version, signature_fingerprint, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

const userDefinedFunctionCatalogSourceColumns = `function_id, active_revision, namespace_version, name, owner, args, ` +
	catalog.UserDefinedFunctionArgumentTypesSQL + `, canonical_input_descriptor, return_descriptor, signature_key_schema_version, signature_fingerprint, retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

// userDefinedFunctionCatalogRevisionSourceColumns preserves immutable heads
// from a snapshot that has the revision columns but predates the exact
// descriptor identity fields. Python rows from such a snapshot remain
// rejected by the current resolver because their identity metadata is absent;
// ordinary SQL rows retain their historical catalog behavior.
const userDefinedFunctionCatalogRevisionSourceColumns = `function_id, active_revision, namespace_version, name, owner, args, ` +
	catalog.UserDefinedFunctionArgumentTypesSQL + `, '', '', 0, '', retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

// userDefinedFunctionCatalogLegacySourceColumns is used for snapshots whose
// historical table predates immutable revision heads. Such rows remain
// usable for existing SQL catalog behavior, while the new Python resolver
// rejects the zero revision and requires an explicit CREATE/replace.
const userDefinedFunctionCatalogLegacySourceColumns = `function_id, 0, 0, name, owner, args, ` +
	catalog.UserDefinedFunctionArgumentTypesSQL + `, '', '', 0, '', retType, body, language, db, definer, modified_time, created_time, type, security_type, comment, character_set_client, collation_connection, database_collation, sql_mode`

const functionRevisionCatalogColumns = `function_id, revision, namespace_version, name, args, arg_types, rettype, body, language, definition_schema_version, abi_contract, adapter_version, artifact_digest, environment_digest, sdk_version, null_policy, volatility, definition_fingerprint, created_time, security_type`

// isCurrentSchemaUserDefinedFunctionCatalog identifies the catalog whose
// schema is owned by the running binary. Restoring a historical CREATE TABLE
// for it would remove arg_types even when the current tenant-upgrade state is
// already complete.
func isCurrentSchemaUserDefinedFunctionCatalog(tblInfo *tableInfo) bool {
	return tblInfo != nil && tblInfo.dbName == moCatalog && tblInfo.tblName == "mo_user_defined_function"
}

// isCurrentFunctionRevisionCatalog identifies the immutable shared revision
// catalog whose schema is owned by the running binary. A historical CREATE
// TABLE must never replace it: SQL and Python resolvers need every contract
// column to reject stale or partially restored definitions before execution.
func isCurrentFunctionRevisionCatalog(tblInfo *tableInfo) bool {
	return tblInfo != nil && tblInfo.dbName == moCatalog && tblInfo.tblName == "mo_function_revisions"
}

// restoreUserDefinedFunctionCatalogWithCurrentSchema restores historical UDF
// rows into the current catalog shape. The source may predate arg_types, so
// the copy deliberately derives it from args through the same ByteJson SQL
// expression used by the v4.0.6 backfill. This keeps restore independent of
// the snapshot's DDL generation and preserves exact overload identities.
func restoreUserDefinedFunctionCatalogWithCurrentSchema(
	ctx context.Context,
	bh BackgroundExec,
	sourceSnapshot string,
	sourceAccount uint32,
	targetAccount uint32,
	sourceCreateSQL string,
) error {
	targetCtx := defines.AttachAccountId(ctx, targetAccount)
	tableName := qualifiedTableName(moCatalog, "mo_user_defined_function")
	if err := bh.Exec(targetCtx, dropTableIfExistsSQL(moCatalog, "mo_user_defined_function")); err != nil {
		return err
	}
	if err := bh.Exec(targetCtx, MoCatalogMoUserDefinedFunctionDDL); err != nil {
		return err
	}

	sourceColumns := userDefinedFunctionCatalogLegacySourceColumns
	lowerCreateSQL := strings.ToLower(sourceCreateSQL)
	if strings.Contains(lowerCreateSQL, "active_revision") && strings.Contains(lowerCreateSQL, "namespace_version") {
		sourceColumns = userDefinedFunctionCatalogRevisionSourceColumns
		if strings.Contains(lowerCreateSQL, "canonical_input_descriptor") &&
			strings.Contains(lowerCreateSQL, "return_descriptor") &&
			strings.Contains(lowerCreateSQL, "signature_key_schema_version") &&
			strings.Contains(lowerCreateSQL, "signature_fingerprint") {
			sourceColumns = userDefinedFunctionCatalogSourceColumns
		}
	}
	copySQL := fmt.Sprintf(
		"insert into %s (%s) select %s from %s%s",
		tableName,
		userDefinedFunctionCatalogColumns,
		sourceColumns,
		tableName,
		sourceSnapshot,
	)
	if sourceAccount == targetAccount {
		return bh.Exec(targetCtx, copySQL)
	}
	return bh.ExecRestore(targetCtx, copySQL, sourceAccount, targetAccount)
}

// validateRestoredFunctionRevisionCatalog validates the shared immutable
// revision table. Each language owns a distinct typed implementation contract;
// accepting a SQL row as Python (or vice versa) would make restore fail for a
// valid mixed catalog or publish a row that the resolver cannot execute.
func validateRestoredFunctionRevisionCatalog(
	ctx context.Context,
	bh BackgroundExec,
	targetCtx context.Context,
) error {
	tableName := qualifiedTableName(moCatalog, "mo_function_revisions")
	query := fmt.Sprintf(
		"select %s from %s order by function_id, revision;",
		functionRevisionCatalogColumns,
		tableName,
	)
	rows, err := getStringColsList(targetCtx, bh, query, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python revision catalog cannot be validated: %w", err)
	}
	for rowIndex, row := range rows {
		if len(row) != 20 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python revision row %d has %d columns", rowIndex, len(row))
		}
		functionID, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil || functionID == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python revision row %d has an invalid function identity", rowIndex)
		}
		revision, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil || revision == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d has an invalid revision", functionID)
		}
		namespace, err := strconv.ParseUint(row[2], 10, 64)
		if err != nil || namespace == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d has an invalid namespace version", functionID)
		}
		definitionSchema, err := strconv.Atoi(row[9])
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored function %d revision %d has an invalid definition schema", functionID, revision)
		}
		if row[8] == udf.LanguageSQL {
			if err := validateRestoredSQLFunctionRevision(functionID, revision, row, definitionSchema); err != nil {
				return err
			}
			continue
		}
		if row[8] != udf.LanguagePython ||
			definitionSchema != udf.PythonDefinitionSchemaVersion ||
			row[10] != udf.PythonABIContract ||
			row[11] != udf.PythonAdapterVersion ||
			row[14] != udf.PythonSDKVersion ||
			!strings.EqualFold(row[16], "VOLATILE") ||
			!strings.EqualFold(row[19], "INVOKER") {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored function %d revision %d has an unsupported execution contract", functionID, revision)
		}

		body, err := function.DecodePythonRoutineBody(row[7])
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d body is invalid: %w", functionID, revision, err)
		}
		if body.DefinitionSchemaVersion != definitionSchema ||
			body.ABIContract != row[10] ||
			body.AdapterVersion != row[11] ||
			body.ArtifactDigest != row[12] ||
			body.EnvironmentDigest != row[13] ||
			body.SDKVersion != row[14] ||
			body.NullPolicy != row[15] {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d metadata does not match its definition", functionID, revision)
		}
		fingerprint, err := function.PythonRoutineFingerprint(row[7])
		if err != nil || row[17] == "" || row[17] != fingerprint {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d fingerprint mismatch", functionID, revision)
		}
		// `args` retains the shared logical names while `arg_types` in the
		// current Python revision is the exact descriptor identity used for
		// overload selection. Validate both representations independently;
		// comparing arg_types with the logical OID list would reject every
		// current Python revision after restore.
		if _, err := userDefinedFunctionArgumentTypesFromJSON(row[4]); err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d arguments are invalid: %w", functionID, revision, err)
		}
		var args []*function.Arg
		if err := json.Unmarshal([]byte(row[4]), &args); err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d arguments are invalid: %w", functionID, revision, err)
		}
		routine := &function.Udf{
			Language:         udf.LanguagePython,
			Args:             args,
			RetType:          row[6],
			PythonArgTypes:   append([]function.PythonTypeDescriptor(nil), body.ArgTypes...),
			PythonReturnType: body.ReturnType,
		}
		if err := routine.ValidatePythonTypeContract(); err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d type contract mismatch: %w", functionID, revision, err)
		}
		if err := routine.ValidatePythonCatalogSignature(); err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d catalog signature mismatch: %w", functionID, revision, err)
		}
		expectedInput, _, _, err := function.PythonSignatureMetadata(body.ArgTypes, body.ReturnType)
		if err != nil || row[5] != expectedInput {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d revision %d argument descriptor mismatch", functionID, revision)
		}
	}
	return nil
}

func validateRestoredSQLFunctionRevision(functionID, revision uint64, row []string, definitionSchema int) error {
	if definitionSchema != udf.SQLDefinitionSchemaVersion ||
		row[10] != "" || row[11] != "" || row[12] != "" || row[13] != "" || row[14] != "" ||
		row[15] != udf.NullCallHandler || !strings.EqualFold(row[16], "VOLATILE") ||
		!strings.EqualFold(row[19], "DEFINER") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d revision %d has an unsupported execution contract", functionID, revision)
	}
	logicalArgTypes, err := userDefinedFunctionArgumentTypesFromJSON(row[4])
	if err != nil || logicalArgTypes != row[5] {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d revision %d argument metadata is invalid", functionID, revision)
	}
	fingerprint, err := function.SQLRoutineFingerprint(row[7], row[5], row[6])
	if err != nil || row[17] == "" || row[17] != fingerprint {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d revision %d fingerprint mismatch", functionID, revision)
	}
	return nil
}

// validateRestoredFunctionCatalogHeads checks the other half of the shared
// immutable publication. The revision validator proves each copied row is
// self-consistent; this check proves that the active head points at that exact
// row after restore.
func validateRestoredFunctionCatalogHeads(
	ctx context.Context,
	bh BackgroundExec,
	targetCtx context.Context,
) error {
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
	rows, err := getStringColsList(targetCtx, bh, query, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored routine catalog heads cannot be validated: %w", err)
	}
	for rowIndex, row := range rows {
		if len(row) != 16 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored routine catalog head row %d has %d columns", rowIndex, len(row))
		}
		functionID, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil || functionID == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored routine catalog head row %d has an invalid function identity", rowIndex)
		}
		activeRevision, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored routine function %d has an invalid active revision: %v", functionID, err)
		}
		namespaceVersion, err := strconv.ParseUint(row[2], 10, 64)
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored routine function %d has an invalid namespace version: %v", functionID, err)
		}
		language := strings.ToLower(row[12])
		if (language == udf.LanguageSQL || language == udf.LanguagePython) && activeRevision == 0 && namespaceVersion == 0 {
			// Legacy SQL rows predate the shared revision table and retain their
			// old execution path. Legacy Python demo rows are preserved as inert
			// catalog data as well: the current Python resolver has no execution
			// path for them and rejects them before user code, while DROP/CREATE
			// can replace the row explicitly. A current head is validated below.
			continue
		}
		if activeRevision == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored %s function %d has no active immutable revision", language, functionID)
		}
		if namespaceVersion == 0 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored %s function %d has an invalid namespace version", language, functionID)
		}
		revision, err := strconv.ParseUint(row[3], 10, 64)
		if err != nil || revision != activeRevision {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored %s function %d active revision does not exist", language, functionID)
		}
		revisionNamespace, err := strconv.ParseUint(row[4], 10, 64)
		if err != nil || revisionNamespace != namespaceVersion {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored %s function %d head namespace does not match its revision", language, functionID)
		}
		if language == udf.LanguageSQL {
			if row[10] == "" || row[11] == "" {
				return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d has incomplete active revision", functionID)
			}
			if !strings.EqualFold(row[14], "DEFINER") || !strings.EqualFold(row[15], "DEFINER") {
				return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d has inconsistent security contract", functionID)
			}
			expectedFingerprint, fingerprintErr := function.SQLRoutineFingerprint(row[11], row[9], row[10])
			if fingerprintErr != nil || row[13] == "" || row[13] != expectedFingerprint {
				return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored SQL function %d head fingerprint does not match its active revision", functionID)
			}
			continue
		}
		if language != udf.LanguagePython {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored function %d has unsupported language %q", functionID, row[12])
		}
		if row[5] == "" || row[6] == "" || row[7] == "" || row[8] == "" || row[9] == "" || row[10] == "" || row[11] == "" || row[13] == "" ||
			!strings.EqualFold(row[14], "INVOKER") || !strings.EqualFold(row[15], "INVOKER") ||
			!strings.EqualFold(row[14], row[15]) {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d has incomplete head identity", functionID)
		}
		body, err := function.DecodePythonRoutineBody(row[11])
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d head body is invalid: %w", functionID, err)
		}
		input, output, signature, err := function.PythonSignatureMetadata(body.ArgTypes, body.ReturnType)
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d head signature is invalid: %w", functionID, err)
		}
		bodyFingerprint, fingerprintErr := function.PythonRoutineFingerprint(row[11])
		if fingerprintErr != nil || row[13] != bodyFingerprint ||
			row[5] != input || row[6] != output || row[7] != strconv.Itoa(udf.PythonSignatureKeySchemaVersion) || row[8] != signature || row[9] != input {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python function %d head identity does not match its active revision", functionID)
		}
	}
	return nil
}

// restoreFunctionRevisionCatalogWithCurrentSchema restores only a revision
// table that already carries the current immutable shared contract. Older/demo
// revision tables are intentionally left empty after the current table is
// created. Their rows cannot be losslessly interpreted by the new resolver, and
// the corresponding active head will be rejected until the function is
// explicitly recreated with the current DDL.
func restoreFunctionRevisionCatalogWithCurrentSchema(
	ctx context.Context,
	bh BackgroundExec,
	sourceSnapshot string,
	sourceAccount uint32,
	targetAccount uint32,
	sourceCreateSQL string,
) error {
	targetCtx := defines.AttachAccountId(ctx, targetAccount)
	tableName := qualifiedTableName(moCatalog, "mo_function_revisions")
	if err := bh.Exec(targetCtx, dropTableIfExistsSQL(moCatalog, "mo_function_revisions")); err != nil {
		return err
	}
	if err := bh.Exec(targetCtx, MoCatalogMoFunctionRevisionDDL); err != nil {
		return err
	}

	// Requiring every current column makes the refusal stable for all older
	// layouts. Checking the CREATE text also avoids querying a historical table
	// with columns that only exist in the target schema.
	lowerCreateSQL := strings.ToLower(sourceCreateSQL)
	for _, column := range strings.Split(functionRevisionCatalogColumns, ", ") {
		if !strings.Contains(lowerCreateSQL, strings.ToLower(column)) {
			return nil
		}
	}

	copySQL := fmt.Sprintf(
		"insert into %s (%s) select %s from %s%s",
		tableName,
		functionRevisionCatalogColumns,
		functionRevisionCatalogColumns,
		tableName,
		sourceSnapshot,
	)
	if sourceAccount == targetAccount {
		if err := bh.Exec(targetCtx, copySQL); err != nil {
			return err
		}
	} else if err := bh.ExecRestore(targetCtx, copySQL, sourceAccount, targetAccount); err != nil {
		return err
	}
	// A current revision table is executable metadata. Validate the copied
	// rows after the transport has completed so a backup/restore or cross
	// account path cannot publish a row whose body, logical catalog columns,
	// digest, or current ABI disagree. The resolver repeats this check at bind
	// time; restore must fail early as well, before a bad head can be reused.
	return validateRestoredFunctionRevisionCatalog(ctx, bh, targetCtx)
}

func restoreSystemCatalogsAfterObjects(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotTS int64,
	sourceAccount uint32,
	targetAccount uint32,
) error {
	if err := validateSystemCatalogRestoreHandlers(ctx); err != nil {
		return err
	}
	restoreCtx := &systemCatalogRestoreContext{
		ctx:           ctx,
		sid:           sid,
		bh:            bh,
		snapshotTS:    snapshotTS,
		sourceAccount: sourceAccount,
		targetAccount: targetAccount,
	}
	for _, entry := range systemCatalogPostRestoreHandlers {
		if err := entry.handler(restoreCtx); err != nil {
			return err
		}
	}
	if bh == nil {
		return nil
	}
	if err := validateRestoredFunctionCatalogHeads(
		ctx,
		bh,
		defines.AttachAccountId(ctx, targetAccount),
	); err != nil {
		return err
	}
	return publishRestoredPythonArtifacts(
		ctx,
		sid,
		bh,
		defines.AttachAccountId(ctx, targetAccount),
		targetAccount,
	)
}

// publishRestoredPythonArtifacts rebuilds the account-scoped immutable
// artifact objects after a catalog restore. Catalog rows and FileService
// objects are separate persistence domains; copying only the revision table
// would leave a syntactically valid head that cannot execute after restore.
// This runs after all catalog identity/head checks, uses the exact restored
// revision body, and publishes write-once objects before restore returns.
func publishRestoredPythonArtifacts(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	targetCtx context.Context,
	targetAccount uint32,
) error {
	rows, err := getStringColsList(
		targetCtx,
		bh,
		"select language, artifact_digest, body from mo_catalog.mo_function_revisions order by function_id, revision;",
		0, 1, 2,
	)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python artifacts cannot be inspected: %w", err)
	}
	needStore := false
	for rowIndex, row := range rows {
		if len(row) != 3 {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored revision row %d has %d artifact columns", rowIndex, len(row))
		}
		if row[0] == udf.LanguagePython {
			needStore = true
			break
		}
	}
	if !needStore {
		return nil
	}
	pu := getPuIfPresent(sid)
	if pu == nil || pu.FileService == nil {
		return moerr.NewNotSupportedNoCtx("Python artifact store is required to restore a current Python revision")
	}
	store, err := pythonudf.NewFileArtifactStore(pu.FileService, pythonudf.DefaultMaxArtifactBytes)
	if err != nil {
		return err
	}
	for rowIndex, row := range rows {
		if row[0] != udf.LanguagePython {
			continue
		}
		body, decodeErr := function.DecodePythonRoutineBody(row[2])
		if decodeErr != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python artifact row %d is invalid: %w", rowIndex, decodeErr)
		}
		if body.ArtifactDigest != row[1] {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: restored Python artifact row %d digest does not match its revision", rowIndex)
		}
		if _, publishErr := store.Publish(ctx, uint64(targetAccount), body.Handler, body.Source); publishErr != nil {
			return publishErr
		}
	}
	return nil
}

func validateSystemCatalogRestoreHandlers(ctx context.Context) error {
	registered := make(map[string]struct{}, len(systemCatalogPostRestoreHandlers))
	for _, entry := range systemCatalogPostRestoreHandlers {
		if systemCatalogRestorePolicies[entry.tableName] != systemCatalogRestoreRebuild {
			return moerr.NewInternalErrorf(ctx, "catalog restore handler for %s has no rebuild policy", entry.tableName)
		}
		if _, exists := registered[entry.tableName]; exists {
			return moerr.NewInternalErrorf(ctx, "catalog restore rebuild for %s has multiple handlers", entry.tableName)
		}
		registered[entry.tableName] = struct{}{}
	}
	for tableName, policy := range systemCatalogRestorePolicies {
		if policy == systemCatalogRestoreRebuild {
			if _, ok := registered[tableName]; !ok {
				return moerr.NewInternalErrorf(ctx, "catalog restore rebuild for %s has no handler", tableName)
			}
		}
	}
	return nil
}

type catalogObjectName struct {
	database   string
	name       string
	objectType string
}

type catalogObjectIdentity struct {
	id uint64
	catalogObjectName
}

type catalogRestoreIdentityMap struct {
	databaseIDs map[uint64]uint64
	objectIDs   map[uint64]uint64
}

// catalogRestorePrincipalIdentityMap binds tenant-local identities from the
// historical account to the account reconstructed by restore. User and role
// IDs normally survive because their catalog tables are copied, but restore
// must not rely on that incidental equality when an account is re-created.
type catalogRestorePrincipalIdentityMap struct {
	userIDs map[uint32]uint32
	roleIDs map[uint32]uint32
}

type publicationRestoreIdentity struct {
	accountID uint32
	userID    uint32
	roleID    uint32
}

type rolePrivilegeRestoreRow struct {
	roleID          int64
	roleName        string
	objectType      string
	objectID        uint64
	privilegeID     int64
	privilegeName   string
	privilegeLevel  string
	operationUserID uint64
	grantedTime     string
	withGrantOption bool
}

func restoreRolePrivilegesAfterObjects(restoreCtx *systemCatalogRestoreContext) error {
	identityMap, err := loadCatalogRestoreIdentityMap(restoreCtx)
	if err != nil {
		return err
	}
	rows, err := loadRolePrivilegesAtSnapshot(restoreCtx)
	if err != nil {
		return err
	}

	kept := rows[:0]
	for _, row := range rows {
		newObjectID, found, remapErr := remapRolePrivilegeObjectID(row, identityMap)
		if remapErr != nil {
			return remapErr
		}
		if !found {
			// Some source objects (for example, subscription databases) are
			// deliberately omitted by bulk account restore. Their grants must be
			// omitted too; retaining the source ID could authorize an unrelated
			// target object after a future ID allocation.
			continue
		}
		row.objectID = newObjectID
		kept = append(kept, row)
	}

	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)
	if err = restoreCtx.bh.Exec(targetCtx, "delete from mo_catalog.mo_role_privs"); err != nil {
		return err
	}
	if len(kept) == 0 {
		return nil
	}

	insertPrefix := "insert into mo_catalog.mo_role_privs(" +
		"role_id,role_name,obj_type,obj_id,privilege_id,privilege_name," +
		"privilege_level,operation_user_id,granted_time,with_grant_option) values "
	for start := 0; start < len(kept); start += rolePrivilegeRestoreInsertBatchSize {
		end := min(start+rolePrivilegeRestoreInsertBatchSize, len(kept))
		values := make([]string, 0, end-start)
		for _, row := range kept[start:end] {
			values = append(values, fmt.Sprintf(
				"(%d,%s,%s,%d,%d,%s,%s,%d,%s,%t)",
				row.roleID,
				quoteSQLStringLiteral(row.roleName),
				quoteSQLStringLiteral(row.objectType),
				row.objectID,
				row.privilegeID,
				quoteSQLStringLiteral(row.privilegeName),
				quoteSQLStringLiteral(row.privilegeLevel),
				row.operationUserID,
				quoteSQLStringLiteral(row.grantedTime),
				row.withGrantOption,
			))
		}
		if err = restoreCtx.bh.Exec(targetCtx, insertPrefix+strings.Join(values, ",")); err != nil {
			return err
		}
	}
	return nil
}

func loadCatalogRestoreIdentityMap(restoreCtx *systemCatalogRestoreContext) (*catalogRestoreIdentityMap, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)

	sourceDatabaseSQL := fmt.Sprintf(
		"select cast(dat_id as char), datname from mo_catalog.mo_database {MO_TS = %d} "+
			"where account_id = %d order by dat_id",
		restoreCtx.snapshotTS, restoreCtx.sourceAccount,
	)
	sourceDatabases, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sourceDatabaseSQL, restoreCtx.sourceAccount, restoreCtx.targetAccount, 0, 1,
	)
	if err != nil {
		return nil, err
	}
	targetDatabases, err := getStringColsList(
		targetCtx, restoreCtx.bh,
		fmt.Sprintf(
			"select cast(dat_id as char), datname from mo_catalog.mo_database where account_id = %d order by dat_id",
			restoreCtx.targetAccount,
		), 0, 1,
	)
	if err != nil {
		return nil, err
	}

	sourceObjectSQL := fmt.Sprintf(
		"select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind "+
			"from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d order by rel_id",
		restoreCtx.snapshotTS, restoreCtx.sourceAccount,
	)
	sourceObjects, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sourceObjectSQL, restoreCtx.sourceAccount, restoreCtx.targetAccount, 0, 1, 2, 3,
	)
	if err != nil {
		return nil, err
	}
	targetObjects, err := getStringColsList(
		targetCtx, restoreCtx.bh,
		fmt.Sprintf(
			"select cast(coalesce(rel_logical_id, rel_id) as char), reldatabase, relname, relkind "+
				"from mo_catalog.mo_tables where account_id = %d order by rel_id",
			restoreCtx.targetAccount,
		), 0, 1, 2, 3,
	)
	if err != nil {
		return nil, err
	}

	return buildCatalogRestoreIdentityMap(sourceDatabases, targetDatabases, sourceObjects, targetObjects)
}

func loadCatalogRestorePrincipalIdentityMap(
	restoreCtx *systemCatalogRestoreContext,
) (*catalogRestorePrincipalIdentityMap, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	targetCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.targetAccount)

	sourceUsers, err := getStringColsListFromTS(
		sourceCtx,
		restoreCtx.bh,
		fmt.Sprintf(
			"select cast(user_id as char), user_name from mo_catalog.mo_user {MO_TS = %d} order by user_id",
			restoreCtx.snapshotTS,
		),
		restoreCtx.sourceAccount,
		restoreCtx.targetAccount,
		0,
		1,
	)
	if err != nil {
		return nil, err
	}
	targetUsers, err := getStringColsList(
		targetCtx,
		restoreCtx.bh,
		"select cast(user_id as char), user_name from mo_catalog.mo_user order by user_id",
		0,
		1,
	)
	if err != nil {
		return nil, err
	}

	sourceRoles, err := getStringColsListFromTS(
		sourceCtx,
		restoreCtx.bh,
		fmt.Sprintf(
			"select cast(role_id as char), role_name from mo_catalog.mo_role {MO_TS = %d} order by role_id",
			restoreCtx.snapshotTS,
		),
		restoreCtx.sourceAccount,
		restoreCtx.targetAccount,
		0,
		1,
	)
	if err != nil {
		return nil, err
	}
	targetRoles, err := getStringColsList(
		targetCtx,
		restoreCtx.bh,
		"select cast(role_id as char), role_name from mo_catalog.mo_role order by role_id",
		0,
		1,
	)
	if err != nil {
		return nil, err
	}

	userIDs, err := buildCatalogRestoreNamedIdentityMap(sourceUsers, targetUsers)
	if err != nil {
		return nil, err
	}
	roleIDs, err := buildCatalogRestoreNamedIdentityMap(sourceRoles, targetRoles)
	if err != nil {
		return nil, err
	}
	return &catalogRestorePrincipalIdentityMap{userIDs: userIDs, roleIDs: roleIDs}, nil
}

func buildCatalogRestoreNamedIdentityMap(sourceRows, targetRows [][]string) (map[uint32]uint32, error) {
	targetIDs := make(map[string]uint32, len(targetRows))
	for _, row := range targetRows {
		id, err := parseCatalogUint32ID(row)
		if err != nil {
			return nil, err
		}
		targetIDs[row[1]] = id
	}

	identityMap := make(map[uint32]uint32, len(sourceRows))
	for _, row := range sourceRows {
		sourceID, err := parseCatalogUint32ID(row)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetIDs[row[1]]; ok {
			identityMap[sourceID] = targetID
		}
	}
	return identityMap, nil
}

func parseCatalogUint32ID(row []string) (uint32, error) {
	id, err := parseCatalogID(row, 2)
	if err != nil {
		return 0, err
	}
	if id > uint64(^uint32(0)) {
		return 0, moerr.NewInternalErrorNoCtx("catalog identity exceeds uint32")
	}
	return uint32(id), nil
}

func resolvePublicationRestoreIdentity(
	ctx context.Context,
	pubInfo *pubsub.PubInfo,
	targetAccounts map[string]*pubsub.AccountInfo,
	principalMap *catalogRestorePrincipalIdentityMap,
) (publicationRestoreIdentity, error) {
	targetAccount, ok := targetAccounts[pubInfo.PubAccountName]
	if !ok || targetAccount == nil || targetAccount.Id < 0 {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: target account %s does not exist",
			pubInfo.PubName,
			pubInfo.PubAccountName,
		)
	}
	targetUserID, ok := principalMap.userIDs[pubInfo.Creator]
	if !ok {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: creator user %d does not exist in target account %s",
			pubInfo.PubName,
			pubInfo.Creator,
			pubInfo.PubAccountName,
		)
	}
	targetRoleID, ok := principalMap.roleIDs[pubInfo.Owner]
	if !ok {
		return publicationRestoreIdentity{}, moerr.NewInternalErrorf(
			ctx,
			"cannot restore publication %s: owner role %d does not exist in target account %s",
			pubInfo.PubName,
			pubInfo.Owner,
			pubInfo.PubAccountName,
		)
	}
	return publicationRestoreIdentity{
		accountID: uint32(targetAccount.Id),
		userID:    targetUserID,
		roleID:    targetRoleID,
	}, nil
}

func buildCatalogRestoreIdentityMap(
	sourceDatabases [][]string,
	targetDatabases [][]string,
	sourceObjects [][]string,
	targetObjects [][]string,
) (*catalogRestoreIdentityMap, error) {
	targetDatabaseIDs := make(map[string]uint64, len(targetDatabases))
	for _, row := range targetDatabases {
		id, err := parseCatalogID(row, 2)
		if err != nil {
			return nil, err
		}
		targetDatabaseIDs[row[1]] = id
	}
	databaseIDs := make(map[uint64]uint64, len(sourceDatabases))
	for _, row := range sourceDatabases {
		sourceID, err := parseCatalogID(row, 2)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetDatabaseIDs[row[1]]; ok {
			databaseIDs[sourceID] = targetID
		}
	}

	targetObjectIDs := make(map[catalogObjectName]uint64, len(targetObjects))
	for _, row := range targetObjects {
		identity, err := parseCatalogObjectIdentity(row)
		if err != nil {
			return nil, err
		}
		targetObjectIDs[identity.catalogObjectName] = identity.id
	}
	objectIDs := make(map[uint64]uint64, len(sourceObjects))
	for _, row := range sourceObjects {
		identity, err := parseCatalogObjectIdentity(row)
		if err != nil {
			return nil, err
		}
		if targetID, ok := targetObjectIDs[identity.catalogObjectName]; ok {
			objectIDs[identity.id] = targetID
		}
	}

	return &catalogRestoreIdentityMap{databaseIDs: databaseIDs, objectIDs: objectIDs}, nil
}

func parseCatalogID(row []string, expectedColumns int) (uint64, error) {
	if len(row) != expectedColumns {
		return 0, moerr.NewInternalErrorNoCtx("invalid catalog identity row")
	}
	id, err := strconv.ParseUint(row[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func parseCatalogObjectIdentity(row []string) (catalogObjectIdentity, error) {
	id, err := parseCatalogID(row, 4)
	if err != nil {
		return catalogObjectIdentity{}, err
	}
	objectType := objectTypeTable.String()
	if row[3] == catalog.SystemViewRel {
		objectType = objectTypeView.String()
	}
	return catalogObjectIdentity{
		id: id,
		catalogObjectName: catalogObjectName{
			database:   row[1],
			name:       row[2],
			objectType: objectType,
		},
	}, nil
}

func loadRolePrivilegesAtSnapshot(restoreCtx *systemCatalogRestoreContext) ([]rolePrivilegeRestoreRow, error) {
	sourceCtx := defines.AttachAccountId(restoreCtx.ctx, restoreCtx.sourceAccount)
	sql := fmt.Sprintf(
		"select cast(role_id as char), role_name, obj_type, cast(obj_id as char), "+
			"cast(privilege_id as char), privilege_name, privilege_level, "+
			"cast(coalesce(operation_user_id, 0) as char), cast(granted_time as char), "+
			"cast(with_grant_option as char) from mo_catalog.mo_role_privs {MO_TS = %d} "+
			"order by role_id, obj_type, obj_id, privilege_id, privilege_level",
		restoreCtx.snapshotTS,
	)
	cols, err := getStringColsListFromTS(
		sourceCtx, restoreCtx.bh, sql, restoreCtx.sourceAccount, restoreCtx.targetAccount,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	)
	if err != nil {
		return nil, err
	}

	rows := make([]rolePrivilegeRestoreRow, 0, len(cols))
	for _, col := range cols {
		if len(col) != 10 {
			return nil, moerr.NewInternalError(restoreCtx.ctx, "invalid mo_role_privs restore row")
		}
		roleID, err := strconv.ParseInt(col[0], 10, 64)
		if err != nil {
			return nil, err
		}
		objectID, err := strconv.ParseUint(col[3], 10, 64)
		if err != nil {
			return nil, err
		}
		privilegeID, err := strconv.ParseInt(col[4], 10, 64)
		if err != nil {
			return nil, err
		}
		operationUserID, err := strconv.ParseUint(col[7], 10, 64)
		if err != nil {
			return nil, err
		}
		withGrantOption, err := strconv.ParseBool(col[9])
		if err != nil {
			if col[9] == "0" || col[9] == "1" {
				withGrantOption = col[9] == "1"
			} else {
				return nil, err
			}
		}
		rows = append(rows, rolePrivilegeRestoreRow{
			roleID:          roleID,
			roleName:        col[1],
			objectType:      col[2],
			objectID:        objectID,
			privilegeID:     privilegeID,
			privilegeName:   col[5],
			privilegeLevel:  col[6],
			operationUserID: operationUserID,
			grantedTime:     col[8],
			withGrantOption: withGrantOption,
		})
	}
	return rows, nil
}

func remapRolePrivilegeObjectID(
	row rolePrivilegeRestoreRow,
	identityMap *catalogRestoreIdentityMap,
) (newObjectID uint64, found bool, err error) {
	// Wildcard account/database/table privileges deliberately use object ID 0.
	// Zero is a sentinel, not an identity, and therefore must never be remapped.
	if row.objectID == objectIDAll {
		return row.objectID, true, nil
	}

	switch row.objectType {
	case objectTypeDatabase.String():
		if row.privilegeLevel != privilegeLevelDatabase.String() {
			return 0, false, moerr.NewInternalErrorNoCtx("nonzero database privilege has an invalid level")
		}
		newObjectID, found = identityMap.databaseIDs[row.objectID]
		return newObjectID, found, nil
	case objectTypeTable.String(), objectTypeView.String():
		switch row.privilegeLevel {
		case privilegeLevelStar.String(), privilegeLevelDatabaseStar.String():
			newObjectID, found = identityMap.databaseIDs[row.objectID]
			return newObjectID, found, nil
		case privilegeLevelDatabaseTable.String(), privilegeLevelTable.String():
			newObjectID, found = identityMap.objectIDs[row.objectID]
			return newObjectID, found, nil
		default:
			return 0, false, moerr.NewInternalErrorNoCtx("nonzero table or view privilege has an invalid level")
		}
	case objectTypeFunction.String():
		// UDF metadata is copied verbatim from mo_user_defined_function, whose
		// function_id is tenant-local. Unlike databases and relations, no DDL
		// recreation allocates a new target identity for this catalog row.
		return row.objectID, true, nil
	default:
		return 0, false, moerr.NewInternalErrorNoCtxf(
			"cannot restore nonzero object ID for privilege object type %s", row.objectType,
		)
	}
}
