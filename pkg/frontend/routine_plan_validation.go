// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package frontend

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

const (
	// Keep dependency validation bounded before allocating or opening a catalog
	// transaction.  The entry limit protects the result/accounting side; the
	// depth limit protects the plan traversal side when a malformed plan nests
	// background queries without contributing many routine entries.
	maxRoutinePlanDependencies    = 1024
	maxRoutinePlanDependencyBytes = 1 << 20
	maxRoutinePlanQueryDepth      = 64
)

type routinePlanCatalogState struct {
	activeRevision    uint64
	namespaceVersion  uint64
	databaseID        uint64
	revision          uint64
	language          string
	volatility        string
	nullPolicy        string
	fingerprint       string
	artifactDigest    string
	environmentDigest string
	securityType      string
	identityChecked   bool
	identityValid     bool
}

func cachedRoutinePlanDependenciesCurrent(ses *Session, cached *cachedPlan) bool {
	if cached == nil {
		return true
	}
	if ses == nil {
		// A cached plan that carries a routine dependency must never be reused
		// without the tenant-aware catalog reader that can validate its exact
		// revision.  Plans without such metadata remain ordinary cache entries.
		for _, p := range cached.plans {
			dependencies, err := routinePlanDependencies(p)
			if err != nil || len(dependencies) != 0 {
				return false
			}
		}
		return true
	}
	ctx := context.Background()
	if tcc := ses.GetTxnCompileCtx(); tcc != nil && tcc.execCtx != nil {
		ctx = tcc.GetContext()
	}
	for _, p := range cached.plans {
		changed, err := validateRoutinePlanDependencies(ctx, ses, p)
		if err != nil || changed {
			return false
		}
	}
	return true
}

func routinePlanDependencies(p *planpb.Plan) ([]*planpb.RoutinePlanDependency, error) {
	if p == nil {
		return nil, nil
	}
	query := p.GetQuery()
	if query == nil {
		return nil, nil
	}

	type queryFrame struct {
		query *planpb.Query
		depth int
		next  int
	}
	initialCapacity := len(query.GetRoutineDependencies())
	if initialCapacity > maxRoutinePlanDependencies {
		initialCapacity = maxRoutinePlanDependencies
	}
	dependencies := make([]*planpb.RoutinePlanDependency, 0, initialCapacity)
	dependencyBytes := 0
	stack := []queryFrame{{query: query}}
	active := map[*planpb.Query]struct{}{query: {}}
	completed := make(map[*planpb.Query]struct{})
	for len(stack) != 0 {
		frameIndex := len(stack) - 1
		frame := &stack[frameIndex]
		if frame.next == 0 {
			frameDependencies := frame.query.GetRoutineDependencies()
			if len(dependencies)+len(frameDependencies) > maxRoutinePlanDependencies {
				return nil, fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine dependency count exceeds %d", maxRoutinePlanDependencies)
			}
			for _, dependency := range frameDependencies {
				dependencySize := 1
				if dependency != nil {
					dependencySize = dependency.ProtoSize()
				}
				if dependencySize > maxRoutinePlanDependencyBytes-dependencyBytes {
					return nil, fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine dependency bytes exceed %d", maxRoutinePlanDependencyBytes)
				}
				dependencyBytes += dependencySize
			}
			dependencies = append(dependencies, frameDependencies...)
		}
		backgroundQueries := frame.query.GetBackgroundQueries()
		if frame.next >= len(backgroundQueries) {
			delete(active, frame.query)
			completed[frame.query] = struct{}{}
			stack = stack[:frameIndex]
			continue
		}
		background := backgroundQueries[frame.next]
		frame.next++
		if background == nil {
			continue
		}
		if frame.depth+1 > maxRoutinePlanQueryDepth {
			return nil, fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine dependency query nesting exceeds %d", maxRoutinePlanQueryDepth)
		}
		if _, exists := active[background]; exists {
			return nil, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine dependency query graph contains a cycle")
		}
		// Shared background queries contribute their immutable dependencies
		// once. Re-expanding a DAG as a tree can turn a small plan into
		// exponential work (even when it contains no routine dependencies).
		if _, exists := completed[background]; exists {
			continue
		}
		active[background] = struct{}{}
		stack = append(stack, queryFrame{query: background, depth: frame.depth + 1})
	}
	return dependencies, nil
}

// validateRoutinePlanDependencies checks a plan against the current catalog
// publication. It is deliberately identity based: execution never resolves a
// routine by name or latest revision, while cache/prepared reuse is rejected
// when the active namespace has moved since the plan was bound.
func validateRoutinePlanDependencies(ctx context.Context, ses FeSession, p *planpb.Plan) (changed bool, err error) {
	dependencies, err := routinePlanDependencies(p)
	if err != nil {
		return false, err
	}
	if len(dependencies) == 0 {
		return false, nil
	}
	if len(dependencies) > maxRoutinePlanDependencies {
		return false, fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine dependency count exceeds %d", maxRoutinePlanDependencies)
	}
	if ses == nil {
		return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine dependency validation has no session")
	}
	accountID := ses.GetAccountId()
	ids := make([]uint64, 0, len(dependencies))
	seen := make(map[uint64]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		if err := validateRoutinePlanDependencyShape(dependency, accountID); err != nil {
			return false, err
		}
		id := dependency.FunctionRef.FunctionId
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if ctx == nil {
		ctx = context.Background()
	}
	ctx = defines.AttachAccountId(ctx, accountID)
	// Background execution is still part of the real SQL call path (for
	// procedures, derived statements, restore and other frontend-owned work).
	// Validate through the execution session so the catalog read carries the
	// same tenant and transaction context as the plan.  GetBackgroundExec is
	// implemented for both client and background sessions; rejecting the latter
	// here would make a valid typed routine fail only when a statement is
	// executed through a nested frontend pipeline.
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine dependency validation could not start: %w", err)
	}
	defer func() { err = finishTxn(ctx, bh, err) }()

	bh.ClearExecResultSet()
	query := routinePlanCatalogSQL(ids, accountID)
	if err = bh.Exec(ctx, query); err != nil {
		return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine dependency validation failed: %w", err)
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	catalogResult, err := exactlyOneCatalogResultSet(ctx, rows, "routine dependency catalog")
	if err != nil {
		return false, err
	}
	states := make(map[uint64]routinePlanCatalogState, len(rows))
	if catalogResult.GetRowCount() != 0 {
		for row := uint64(0); row < catalogResult.GetRowCount(); row++ {
			id, getErr := catalogResult.GetUint64(ctx, row, 0)
			if getErr != nil {
				return false, getErr
			}
			activeRevision, getErr := catalogResult.GetUint64(ctx, row, 1)
			if getErr != nil {
				return false, getErr
			}
			namespaceVersion, getErr := catalogResult.GetUint64(ctx, row, 2)
			if getErr != nil {
				return false, getErr
			}
			databaseID, getErr := catalogResult.GetUint64(ctx, row, 3)
			if getErr != nil {
				return false, getErr
			}
			revision, getErr := catalogResult.GetUint64(ctx, row, 4)
			if getErr != nil {
				return false, getErr
			}
			language, getErr := catalogResult.GetString(ctx, row, 5)
			if getErr != nil {
				return false, getErr
			}
			volatility, getErr := catalogResult.GetString(ctx, row, 6)
			if getErr != nil {
				return false, getErr
			}
			fingerprint, getErr := catalogResult.GetString(ctx, row, 7)
			if getErr != nil {
				return false, getErr
			}
			artifactDigest, getErr := catalogResult.GetString(ctx, row, 8)
			if getErr != nil {
				return false, getErr
			}
			environmentDigest, getErr := catalogResult.GetString(ctx, row, 9)
			if getErr != nil {
				return false, getErr
			}
			canonicalInput, getErr := catalogResult.GetString(ctx, row, 10)
			if getErr != nil {
				return false, getErr
			}
			returnDescriptor, getErr := catalogResult.GetString(ctx, row, 11)
			if getErr != nil {
				return false, getErr
			}
			signatureKeySchemaVersion, getErr := catalogResult.GetInt64(ctx, row, 12)
			if getErr != nil {
				return false, getErr
			}
			signatureFingerprint, getErr := catalogResult.GetString(ctx, row, 13)
			if getErr != nil {
				return false, getErr
			}
			body, getErr := catalogResult.GetString(ctx, row, 14)
			if getErr != nil {
				return false, getErr
			}
			revisionArgTypes, getErr := catalogResult.GetString(ctx, row, 15)
			if getErr != nil {
				return false, getErr
			}
			revisionRetType, getErr := catalogResult.GetString(ctx, row, 16)
			if getErr != nil {
				return false, getErr
			}
			definitionSchema, getErr := catalogResult.GetInt64(ctx, row, 17)
			if getErr != nil {
				return false, getErr
			}
			revisionABI, getErr := catalogResult.GetString(ctx, row, 18)
			if getErr != nil {
				return false, getErr
			}
			revisionAdapter, getErr := catalogResult.GetString(ctx, row, 19)
			if getErr != nil {
				return false, getErr
			}
			revisionSDK, getErr := catalogResult.GetString(ctx, row, 20)
			if getErr != nil {
				return false, getErr
			}
			revisionNullPolicy, getErr := catalogResult.GetString(ctx, row, 21)
			if getErr != nil {
				return false, getErr
			}
			revisionSecurityType, getErr := catalogResult.GetString(ctx, row, 22)
			if getErr != nil {
				return false, getErr
			}
			baseSecurityType, getErr := catalogResult.GetString(ctx, row, 23)
			if getErr != nil {
				return false, getErr
			}
			if !strings.EqualFold(revisionSecurityType, baseSecurityType) {
				return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine %d has inconsistent revision and identity security contracts", id)
			}
			if strings.EqualFold(language, udf.LanguagePython) && !strings.EqualFold(revisionSecurityType, "INVOKER") {
				return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python routine catalog security type is not INVOKER")
			}
			if strings.EqualFold(language, udf.LanguageSQL) && !strings.EqualFold(revisionSecurityType, "DEFINER") {
				return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: SQL routine catalog security type is not DEFINER")
			}
			identityValid := routinePlanCatalogIdentityValidForLanguage(
				language,
				body, fingerprint, signatureKeySchemaVersion, canonicalInput,
				returnDescriptor, signatureFingerprint, revisionArgTypes,
				revisionRetType, definitionSchema, revisionABI, revisionAdapter,
				revisionSDK, revisionNullPolicy,
			)
			if _, duplicate := states[id]; duplicate {
				return false, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine catalog returned duplicate state for function %d", id)
			}
			states[id] = routinePlanCatalogState{
				activeRevision: activeRevision, namespaceVersion: namespaceVersion,
				databaseID: databaseID,
				revision:   revision, language: language, volatility: volatility, nullPolicy: revisionNullPolicy, fingerprint: fingerprint,
				artifactDigest: artifactDigest, environmentDigest: environmentDigest,
				securityType:    revisionSecurityType,
				identityChecked: true, identityValid: identityValid,
			}
		}
	}
	return routinePlanDependenciesChanged(dependencies, states), nil
}

// routinePlanCatalogIdentityValid verifies the complete immutable revision
// contract before a cached plan can be reused.  Keeping this check separate
// from the SQL row decoder makes the important tamper cases directly
// testable: changing source, a descriptor, an ABI field, or a legacy logical
// return type while retaining the old fingerprint must invalidate the plan.
func routinePlanCatalogIdentityValid(
	rawBody, fingerprint string,
	signatureKeySchemaVersion int64,
	canonicalInput, returnDescriptor, signatureFingerprint, revisionArgTypes,
	revisionRetType string,
	definitionSchema int64,
	revisionABI, revisionAdapter, revisionSDK, revisionNullPolicy string,
) bool {
	return routinePlanCatalogIdentityValidForLanguage(
		udf.LanguagePython,
		rawBody, fingerprint, signatureKeySchemaVersion, canonicalInput,
		returnDescriptor, signatureFingerprint, revisionArgTypes, revisionRetType,
		definitionSchema, revisionABI, revisionAdapter, revisionSDK, revisionNullPolicy,
	)
}

func routinePlanCatalogIdentityValidForLanguage(
	language string,
	rawBody, fingerprint string,
	signatureKeySchemaVersion int64,
	canonicalInput, returnDescriptor, signatureFingerprint, revisionArgTypes,
	revisionRetType string,
	definitionSchema int64,
	revisionABI, revisionAdapter, revisionSDK, revisionNullPolicy string,
) bool {
	if strings.EqualFold(language, udf.LanguageSQL) {
		expectedFingerprint, err := function.SQLRoutineFingerprint(rawBody, revisionArgTypes, revisionRetType)
		return err == nil &&
			fingerprint == expectedFingerprint &&
			definitionSchema == int64(udf.SQLDefinitionSchemaVersion) &&
			revisionABI == "" && revisionAdapter == "" && revisionSDK == "" &&
			revisionNullPolicy == udf.NullCallHandler
	}
	if !strings.EqualFold(language, udf.LanguagePython) {
		return false
	}
	decoded, err := function.DecodePythonRoutineBody(rawBody)
	if err != nil {
		return false
	}
	expectedInput, expectedReturn, expectedFingerprint, err := function.PythonSignatureMetadata(decoded.ArgTypes, decoded.ReturnType)
	if err != nil {
		return false
	}
	bodyFingerprint, err := function.PythonRoutineBodyFingerprint(decoded)
	if err != nil {
		return false
	}
	return bodyFingerprint == fingerprint &&
		signatureKeySchemaVersion == udf.PythonSignatureKeySchemaVersion &&
		canonicalInput == expectedInput && returnDescriptor == expectedReturn && signatureFingerprint == expectedFingerprint &&
		revisionArgTypes == expectedInput &&
		routineRevisionMetadataMatches(decoded, definitionSchema, revisionABI, revisionAdapter, revisionSDK, revisionNullPolicy) &&
		routineRevisionReturnTypeMatches(revisionRetType, decoded.ReturnType.Type().Oid)
}

func routinePlanCatalogSQL(ids []uint64, accountID uint32) string {
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = strconv.FormatUint(id, 10)
	}
	return fmt.Sprintf(`select f.function_id, f.active_revision, f.namespace_version, d.dat_id,
		r.revision, r.language, r.volatility, r.definition_fingerprint, r.artifact_digest,
		r.environment_digest, f.canonical_input_descriptor, f.return_descriptor,
		f.signature_key_schema_version, f.signature_fingerprint, r.body, r.arg_types,
		r.rettype, r.definition_schema_version, r.abi_contract, r.adapter_version,
			r.sdk_version, r.null_policy, r.security_type, f.security_type
		from mo_catalog.mo_user_defined_function f
		join mo_catalog.mo_database d on d.datname = f.db and d.account_id = %d
		join mo_catalog.mo_function_revisions r
			on r.function_id = f.function_id and r.revision = f.active_revision
			and r.namespace_version = f.namespace_version
		where f.function_id in (%s);`, accountID, strings.Join(values, ","))
}

func routineRevisionMetadataMatches(
	body function.PythonRoutineBody,
	definitionSchema int64,
	abi, adapter, sdk, nullPolicy string,
) bool {
	return definitionSchema == int64(udf.PythonDefinitionSchemaVersion) &&
		abi == udf.PythonABIContract && adapter == udf.PythonAdapterVersion &&
		sdk == udf.PythonSDKVersion && nullPolicy == body.NullPolicy &&
		body.ABIContract == abi && body.AdapterVersion == adapter && body.SDKVersion == sdk
}

func routineRevisionReturnTypeMatches(rettype string, oid types.T) bool {
	if strings.EqualFold(rettype, oid.String()) {
		return true
	}
	return strings.EqualFold(rettype, "decimal") && (oid == types.T_decimal64 || oid == types.T_decimal128)
}

func validateRoutinePlanDependencyShape(dependency *planpb.RoutinePlanDependency, accountID uint32) error {
	if dependency == nil || dependency.FunctionRef == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine plan dependency has no FunctionRef")
	}
	ref := dependency.FunctionRef
	if ref.FunctionId == 0 || ref.Revision == 0 || ref.NamespaceVersion == 0 ||
		ref.AccountId != uint64(accountID) || ref.DatabaseId == 0 {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine plan dependency has an invalid catalog identity")
	}
	if dependency.ContractVersion != udf.RoutinePlanContractVersion ||
		!udf.IsSHA256Digest(dependency.DefinitionFingerprint) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine plan dependency contract is incomplete")
	}
	if dependency.Volatility == "" || dependency.NullPolicy == "" || !dependency.MayError || dependency.Leakproof {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine plan dependency semantic contract is incomplete")
	}
	switch strings.ToLower(dependency.Language) {
	case udf.LanguagePython:
		if dependency.SecurityMode != "INVOKER" {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python routine plan dependency must use INVOKER security")
		}
		if !udf.IsSHA256Digest(dependency.ArtifactDigest) || !udf.IsSHA256Digest(dependency.EnvironmentDigest) {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python routine plan dependency integrity is incomplete")
		}
	case udf.LanguageSQL:
		if dependency.SecurityMode == "" {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: SQL routine plan dependency has no security mode")
		}
		if dependency.ArtifactDigest != "" || dependency.EnvironmentDigest != "" {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: SQL routine plan dependency has external integrity fields")
		}
	default:
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine plan dependency language %q is unsupported", dependency.Language)
	}
	return nil
}

func routinePlanDependenciesChanged(
	dependencies []*planpb.RoutinePlanDependency,
	states map[uint64]routinePlanCatalogState,
) bool {
	for _, dependency := range dependencies {
		if dependency == nil || dependency.FunctionRef == nil {
			return true
		}
		ref := dependency.FunctionRef
		state, ok := states[ref.FunctionId]
		if !ok || state.activeRevision == 0 || state.revision == 0 {
			return true
		}
		if state.identityChecked && !state.identityValid {
			return true
		}
		expectedSecurityMode := dependency.SecurityMode
		if expectedSecurityMode == "" {
			return true
		}
		if state.activeRevision != ref.Revision || state.revision != ref.Revision ||
			state.databaseID != ref.DatabaseId ||
			state.namespaceVersion != ref.NamespaceVersion ||
			state.volatility != dependency.Volatility ||
			state.nullPolicy != dependency.NullPolicy ||
			!strings.EqualFold(state.securityType, dependency.SecurityMode) ||
			(dependency.Language == udf.LanguagePython && expectedSecurityMode != "INVOKER") ||
			(dependency.Language == udf.LanguageSQL && expectedSecurityMode == "") ||
			dependency.MayError != true || dependency.Leakproof ||
			!strings.EqualFold(state.language, dependency.Language) ||
			state.fingerprint != dependency.DefinitionFingerprint ||
			state.artifactDigest != dependency.ArtifactDigest ||
			state.environmentDigest != dependency.EnvironmentDigest {
			return true
		}
	}
	return false
}
