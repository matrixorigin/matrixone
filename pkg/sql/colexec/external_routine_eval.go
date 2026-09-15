// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package colexec

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/udf"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ExternalRoutineEval is the physical boundary for a typed RoutineCall. It
// owns argument evaluation, selection compaction/scatter, strict NULL
// guarding, and the result lifetime. The Flight gateway only sees the
// already-admitted selected rows and never owns SQL CASE semantics.
type ExternalRoutineEval struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	call       *planpb.RoutineCall
	resultType types.Type

	parameterExecutor []ExpressionExecutor
	parameterResults  []*vector.Vector
	result            vector.FunctionResultWrapper

	selectedRows       []int64
	selectedParameters []*vector.Vector
	selectedOwned      []*vector.Vector
	selectedResult     vector.FunctionResultWrapper
	selectedNull       *vector.Vector
	generation         uint64
	executionMu        sync.Mutex
	freed              bool
	groupMu            sync.Mutex
	groupStatementID   string
	groupID            string
	groupEpoch         uint64
}

func newExternalRoutineEval(
	proc *process.Process,
	call *planpb.RoutineCall,
	parameters []ExpressionExecutor,
	allocation *vector.AllocationAccountSelection,
) (*ExternalRoutineEval, error) {
	if proc == nil || proc.Base == nil {
		return nil, fmt.Errorf("python udf: evaluator has no process")
	}
	if err := validateRoutineCall(call); err != nil {
		return nil, err
	}
	if len(call.ArgumentTypes) != len(parameters) {
		return nil, fmt.Errorf("python udf: typed routine has %d argument descriptors but %d bound arguments", len(call.ArgumentTypes), len(parameters))
	}
	resultType := planTypeToSQL(call.ReturnType)
	result, err := vector.NewFunctionResultWrapperWithAllocation(resultType, proc.Mp(), allocation)
	if err != nil {
		return nil, err
	}
	return &ExternalRoutineEval{
		mp:                proc.Mp(),
		allocation:        allocation,
		call:              call,
		resultType:        resultType,
		parameterExecutor: parameters,
		parameterResults:  make([]*vector.Vector, len(parameters)),
		result:            result,
	}, nil
}

func validateRoutineCall(call *planpb.RoutineCall) error {
	if call == nil || call.FunctionRef == nil || call.FunctionRef.FunctionId == 0 ||
		call.FunctionRef.DatabaseId == 0 || call.FunctionRef.Revision == 0 || call.FunctionRef.NamespaceVersion == 0 {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed routine call has no exact FunctionRef")
	}
	if call.ReturnType.Id == int32(types.T_any) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed routine call has no return descriptor")
	}
	if call.ContractVersion != udf.PythonPlanContractVersion || !strings.EqualFold(call.Language, udf.LanguagePython) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported typed routine call contract")
	}
	if call.Volatility != "VOLATILE" {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python routine must be VOLATILE")
	}
	if call.MayError != true || call.SecurityMode != "INVOKER" || call.Leakproof {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python routine semantic contract is not supported")
	}
	if call.CallsiteId == "" || len(call.CallsiteId) > 256 || strings.ContainsAny(call.CallsiteId, "\r\n") {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed routine call has no valid callsite id")
	}
	if len(call.Context) != 0 {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed routine call contains execution context")
	}
	python := call.GetPython()
	if python == nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed routine call has no Python implementation")
	}
	if python.Source != "" {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python plan contains source; rebind from the current artifact contract")
	}
	if python.Handler == "" ||
		python.AbiContract != udf.PythonABIContract ||
		python.AdapterVersion != udf.PythonAdapterVersion ||
		python.SdkVersion != udf.PythonSDKVersion ||
		python.DefinitionSchemaVersion != udf.PythonDefinitionSchemaVersion {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python implementation contract is not supported")
	}
	if python.Mode != "SCALAR" && python.Mode != "VECTOR" {
		return fmt.Errorf("python udf: unsupported typed call mode %q", python.Mode)
	}
	if python.NullPolicy != udf.NullCallHandler && python.NullPolicy != udf.NullReturnNull {
		return fmt.Errorf("python udf: unsupported typed NULL policy %q", python.NullPolicy)
	}
	if call.NullPolicy != python.NullPolicy {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed call NULL policy does not match its implementation")
	}
	if !udf.IsSHA256Digest(python.ArtifactDigest) || !udf.IsSHA256Digest(python.EnvironmentDigest) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python implementation has invalid artifact/environment digest")
	}
	if !udf.IsSHA256Digest(python.DefinitionFingerprint) {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python implementation has invalid definition fingerprint")
	}
	arguments := make([]pythonudf.TypeDescriptor, len(call.ArgumentTypes))
	for i, argumentType := range call.ArgumentTypes {
		if argumentType == nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python argument %d is nil", i)
		}
		descriptor, err := function.NewPythonTypeDescriptor(planTypeToSQL(*argumentType))
		if err != nil {
			return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python argument %d is unsupported: %w", i, err)
		}
		arguments[i] = descriptor
	}
	result, err := function.NewPythonTypeDescriptor(planTypeToSQL(call.ReturnType))
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python return descriptor is unsupported: %w", err)
	}
	fingerprint, err := pythonudf.DefinitionFingerprint(
		int(python.DefinitionSchemaVersion), python.Handler, python.Mode, python.NullPolicy,
		python.AbiContract, python.AdapterVersion, python.ArtifactDigest,
		python.EnvironmentDigest, python.SdkVersion, arguments, result,
	)
	if err != nil {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: invalid typed Python definition: %w", err)
	}
	if fingerprint != python.DefinitionFingerprint {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: typed Python definition fingerprint mismatch")
	}

	return nil
}

func (e *ExternalRoutineEval) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.eval(proc, batches, selectList, false)
}

func (e *ExternalRoutineEval) eval(proc *process.Process, batches []*batch.Batch, selectList []bool, transfer bool) (output *vector.Vector, evalErr error) {
	if e == nil || proc == nil || proc.Base == nil {
		return nil, fmt.Errorf("python udf: evaluator has no process")
	}
	e.executionMu.Lock()
	defer e.executionMu.Unlock()
	if e.freed {
		return nil, fmt.Errorf("python udf: evaluator is freed")
	}
	// Transfer under executionMu, before a concurrent Eval/Free can reuse the
	// backing. The wrapper retains its allocation policy for the next result.
	defer func() {
		if transfer && evalErr == nil {
			e.result.SetResultVector(nil)
		}
	}()

	if err := validateRoutineCall(e.call); err != nil {
		return nil, err
	}
	accountID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return nil, err
	}
	if uint64(accountID) != e.call.FunctionRef.AccountId {
		return nil, fmt.Errorf("python udf: FunctionRef account does not match the execution account")
	}
	python := e.call.GetPython()
	rowCount, err := externalRoutineRowCount(python.Mode, batches)
	if err != nil {
		return nil, err
	}
	// Keep the row-domain contract strict even for an empty batch. Returning
	// early before this check would let a stale selection mask cross the
	// physical boundary and hide a relation-position bug in the caller.
	if selectList != nil && len(selectList) != rowCount {
		return nil, fmt.Errorf(
			"external routine selection has %d rows, expected %d",
			len(selectList),
			rowCount,
		)
	}
	if rowCount == 0 {
		if err := e.result.PreExtendAndReset(0); err != nil {
			return nil, err
		}
		return e.result.GetResultVector(), nil
	}

	if noRowsSelected(selectList, rowCount) {
		return e.nullResult(rowCount)
	}

	for i, parameter := range e.parameterExecutor {
		var err error
		e.parameterResults[i], err = parameter.Eval(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	}
	if err := validateExternalRoutineInputs(e.parameterResults, e.call, rowCount); err != nil {
		return nil, err
	}
	e.selectedRows = e.selectedRows[:0]
	for row := 0; row < rowCount; row++ {
		selected := selectList == nil || selectList[row]
		if selected && python.NullPolicy == udf.NullReturnNull && routineHasNullAt(e.parameterResults, row) {
			selected = false
		}
		if selected {
			e.selectedRows = append(e.selectedRows, int64(row))
		}
	}
	if len(e.selectedRows) == 0 {
		return e.nullResult(rowCount)
	}

	inputs, err := e.compactParameters(proc, rowCount)
	if err != nil {
		// compactParameters may have already materialized earlier selected
		// arguments before a later column fails. There is no execution defer
		// until compaction succeeds, so release that partial ownership here.
		e.releaseOwnedParameters()
		return nil, err
	}
	defer e.releaseOwnedParameters()

	selectedResult := e.result
	if len(e.selectedRows) != rowCount {
		if e.selectedResult == nil {
			e.selectedResult, err = vector.NewFunctionResultWrapperWithAllocation(e.resultType, e.mp, e.allocation)
			if err != nil {
				return nil, err
			}
		}
		selectedResult = e.selectedResult
	}
	if err := selectedResult.PreExtendAndReset(len(e.selectedRows)); err != nil {
		return nil, err
	}
	context, err := buildRoutineContext(proc)
	if err != nil {
		return nil, err
	}
	statementContext, err := udf.StatementContextFromMap(context)
	if err != nil {
		return nil, err
	}
	if err := e.attachInvocationGroup(context, proc.QueryId()); err != nil {
		return nil, err
	}
	tuple, err := function.NewInvocationTuple(context, proc.QueryId(), accountID)
	if err != nil {
		return nil, err
	}
	args := make([]types.Type, len(e.call.ArgumentTypes))
	for i, typ := range e.call.ArgumentTypes {
		args[i] = planTypeToSQL(*typ)
	}
	invocation := &udf.Invocation{
		FunctionRef: udf.FunctionRef{
			AccountID:        pythonFunctionAccountID(e.call),
			DatabaseID:       e.call.FunctionRef.DatabaseId,
			FunctionID:       e.call.FunctionRef.FunctionId,
			Revision:         e.call.FunctionRef.Revision,
			NamespaceVersion: e.call.FunctionRef.NamespaceVersion,
		},
		Language:                udf.LanguagePython,
		Handler:                 python.Handler,
		Args:                    args,
		ReturnType:              e.resultType,
		Inputs:                  inputs,
		Length:                  len(e.selectedRows),
		Mode:                    python.Mode,
		NullPolicy:              python.NullPolicy,
		ABIContract:             python.AbiContract,
		AdapterVersion:          python.AdapterVersion,
		SDKVersion:              python.SdkVersion,
		DefinitionSchemaVersion: int(python.DefinitionSchemaVersion),
		ArtifactDigest:          python.ArtifactDigest,
		EnvironmentDigest:       python.EnvironmentDigest,
		DefinitionFingerprint:   python.DefinitionFingerprint,
		Context:                 context,
		CallsiteID:              e.call.CallsiteId,
		MayError:                e.call.MayError,
		SecurityMode:            e.call.SecurityMode,
		Leakproof:               e.call.Leakproof,
		StatementContext:        statementContext,
		SecurityFrame: &udf.SecurityFrame{
			ContractVersion: udf.SecurityFrameContractVersion,
			Mode:            e.call.SecurityMode,
			InvokerUserID:   defines.GetUserId(proc.Ctx),
			InvokerRoleID:   defines.GetRoleId(proc.Ctx),
			EffectiveUserID: defines.GetUserId(proc.Ctx),
			EffectiveRoleID: defines.GetRoleId(proc.Ctx),
		},
		Tuple: tuple,
	}
	if proc.Base.UdfService == nil {
		return nil, fmt.Errorf("RESOURCE_UNAVAILABLE: Python UDF runtime is not enabled")
	}
	if err := proc.Base.UdfService.Execute(proc.Ctx, invocation, selectedResult, proc.Mp()); err != nil {
		return nil, err
	}
	if selectedResult.ResultLength() != len(e.selectedRows) {
		return nil, fmt.Errorf(
			"python udf: runtime produced %d result rows, expected %d",
			selectedResult.ResultLength(),
			len(e.selectedRows),
		)
	}
	if selectedResult.GetResultVector() == nil {
		return nil, fmt.Errorf("python udf: runtime returned no result vector")
	}
	if len(e.selectedRows) == rowCount {
		return e.result.GetResultVector(), nil
	}
	return e.scatter(proc, selectedResult.GetResultVector(), rowCount)
}

// attachInvocationGroup gives one physical evaluator ownership of a reusable
// execution group for one statement. Each invocation still has its own
// InvocationID and monotonically increasing GroupEpoch, so a completed epoch
// cannot be reopened and a later batch cannot consume the previous member's
// resources. ResetForNextQuery discards the group identity; an evaluator that
// has no statement query id uses a fresh one-shot group because it cannot prove
// that two calls belong to the same statement.
func (e *ExternalRoutineEval) attachInvocationGroup(context map[string]string, queryID string) error {
	if context == nil {
		return fmt.Errorf("python udf: invocation context is unavailable")
	}
	e.groupMu.Lock()
	defer e.groupMu.Unlock()
	if queryID == "" {
		group, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("python udf: create execution group fence: %w", err)
		}
		context["group_id"] = "python/" + group.String()
		context["group_epoch"] = "1"
		return nil
	}
	if e.groupID == "" || e.groupStatementID != queryID {
		group, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("python udf: create execution group fence: %w", err)
		}
		e.groupStatementID = queryID
		e.groupID = queryID + "/python/" + group.String()
		e.groupEpoch = 0
	}
	if e.groupEpoch == ^uint64(0) {
		return fmt.Errorf("RESOURCE_EXHAUSTED: Python UDF execution group epoch overflow")
	}
	e.groupEpoch++
	context["group_id"] = e.groupID
	context["group_epoch"] = strconv.FormatUint(e.groupEpoch, 10)
	return nil
}

func pythonFunctionAccountID(call *planpb.RoutineCall) uint64 {
	if call == nil || call.FunctionRef == nil {
		return 0
	}
	return call.FunctionRef.AccountId
}

func (e *ExternalRoutineEval) compactParameters(proc *process.Process, rowCount int) ([]*vector.Vector, error) {
	if len(e.selectedRows) == 0 {
		return nil, nil
	}
	e.selectedParameters = e.selectedParameters[:0]
	for i, input := range e.parameterResults {
		if input == nil {
			return nil, fmt.Errorf("external routine parameter %d is nil", i)
		}
		if input.IsConst() || len(e.selectedRows) == rowCount {
			e.selectedParameters = append(e.selectedParameters, input)
			continue
		}
		selected, err := vector.NewOffHeapVecWithTypeAndAllocation(*input.GetType(), e.allocation)
		if err != nil {
			return nil, err
		}
		selected.SetIsBin(input.GetIsBin())
		if err := selected.Union(input, e.selectedRows, proc.Mp()); err != nil {
			selected.Free(proc.Mp())
			return nil, err
		}
		e.selectedOwned = append(e.selectedOwned, selected)
		e.selectedParameters = append(e.selectedParameters, selected)
	}
	return e.selectedParameters, nil
}

func validateExternalRoutineInputs(inputs []*vector.Vector, call *planpb.RoutineCall, rowCount int) error {
	if len(inputs) != len(call.ArgumentTypes) {
		return fmt.Errorf("python udf: evaluated input count %d does not match routine argument count %d", len(inputs), len(call.ArgumentTypes))
	}
	for index, input := range inputs {
		if input == nil || input.GetType() == nil {
			return fmt.Errorf("python udf: evaluated input vector %d has no type", index)
		}
		if input.Length() == 0 || (!input.IsConst() && input.Length() < rowCount) {
			return fmt.Errorf("python udf: evaluated input vector %d has %d rows, expected at least %d", index, input.Length(), rowCount)
		}
		expected, err := function.NewPythonTypeDescriptor(planTypeToSQL(*call.ArgumentTypes[index]))
		if err != nil {
			return fmt.Errorf("python udf: invalid argument descriptor %d: %w", index, err)
		}
		actual, err := function.NewPythonTypeDescriptor(*input.GetType())
		if err != nil {
			return fmt.Errorf("python udf: evaluated input vector %d has unsupported type: %w", index, err)
		}
		expectedFingerprint, err := expected.Fingerprint()
		if err != nil {
			return err
		}
		actualFingerprint, err := actual.Fingerprint()
		if err != nil {
			return err
		}
		if expectedFingerprint != actualFingerprint {
			return fmt.Errorf("python udf: evaluated input vector %d type %s does not match the frozen descriptor", index, input.GetType().DescString())
		}
	}
	return nil
}

func (e *ExternalRoutineEval) releaseOwnedParameters() {
	for _, selected := range e.selectedOwned {
		if selected != nil {
			selected.Free(e.mp)
		}
	}
	e.selectedOwned = e.selectedOwned[:0]
}

func (e *ExternalRoutineEval) nullResult(rowCount int) (*vector.Vector, error) {
	if err := e.result.PreExtendAndReset(rowCount); err != nil {
		return nil, err
	}
	result := e.result.GetResultVector()
	result.SetAllNulls(rowCount)
	result.SetLength(rowCount)
	return result, nil
}

func (e *ExternalRoutineEval) scatter(proc *process.Process, selected *vector.Vector, rowCount int) (*vector.Vector, error) {
	if err := e.result.PreExtendAndReset(rowCount); err != nil {
		return nil, err
	}
	result := e.result.GetResultVector()
	result.ResetWithSameType()
	if e.selectedNull == nil || *e.selectedNull.GetType() != *selected.GetType() {
		if e.selectedNull != nil {
			e.selectedNull.Free(e.mp)
		}
		e.selectedNull = vector.NewConstNull(*selected.GetType(), 1, proc.Mp())
	}
	selectedRow := 0
	for row := 0; row < rowCount; row++ {
		if selectedRow < len(e.selectedRows) && e.selectedRows[selectedRow] == int64(row) {
			if err := result.UnionOne(selected, int64(selectedRow), proc.Mp()); err != nil {
				return nil, err
			}
			selectedRow++
		} else if err := result.UnionOne(e.selectedNull, 0, proc.Mp()); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (e *ExternalRoutineEval) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.eval(proc, batches, selectList, true)
}

func (e *ExternalRoutineEval) ResetForNextQuery() {
	if e == nil {
		return
	}
	e.executionMu.Lock()
	defer e.executionMu.Unlock()
	if e.freed {
		return
	}
	e.generation++
	e.groupMu.Lock()
	e.groupStatementID = ""
	e.groupID = ""
	e.groupEpoch = 0
	e.groupMu.Unlock()
	for _, parameter := range e.parameterExecutor {
		parameter.ResetForNextQuery()
	}
	e.parameterResults = make([]*vector.Vector, len(e.parameterExecutor))
}

func (e *ExternalRoutineEval) Free() {
	if e == nil {
		return
	}
	e.executionMu.Lock()
	defer e.executionMu.Unlock()
	if e.freed {
		return
	}
	e.freed = true
	for _, parameter := range e.parameterExecutor {
		if parameter != nil {
			parameter.Free()
		}
	}
	if e.result != nil {
		e.result.Free()
	}
	if e.selectedResult != nil {
		e.selectedResult.Free()
	}
	if e.selectedNull != nil {
		e.selectedNull.Free(e.mp)
	}
	e.releaseOwnedParameters()
}

func (e *ExternalRoutineEval) IsColumnExpr() bool { return false }
func (e *ExternalRoutineEval) TypeName() string   { return "external routine eval" }

func routineHasNullAt(inputs []*vector.Vector, row int) bool {
	for _, input := range inputs {
		if input == nil || input.IsNull(uint64(row)) {
			return true
		}
	}
	return false
}

func planTypeToSQL(typ planpb.Type) types.Type {
	return types.NewWithCharset(types.T(typ.Id), typ.Width, typ.Scale, uint8(typ.Charset))
}

func externalRoutineRowCount(mode string, batches []*batch.Batch) (int, error) {
	if len(batches) > 0 {
		// ExpressionExecutor uses the first batch as the logical row domain and
		// uses relation positions to read the other batches. Join operators pass
		// several one-to-one materialized relation batches, so rejecting every
		// multi-batch call would make a Python routine unusable in JOIN ON. A
		// different cardinality is still a contract violation: silently selecting
		// batches[0] would let an argument from another relation lose rows.
		rowCount := -1
		for index, input := range batches {
			if input == nil {
				return 0, fmt.Errorf("external routine input batch %d is nil", index)
			}
			if rowCount < 0 {
				rowCount = input.RowCount()
				continue
			}
			if input.RowCount() != rowCount {
				return 0, fmt.Errorf("python udf: external routine input batch %d has %d rows, expected %d for the logical row domain", index, input.RowCount(), rowCount)
			}
		}
		return rowCount, nil
	}
	if mode == "VECTOR" {
		return 0, fmt.Errorf("python udf: zero-argument VECTOR requires an input batch with num_rows")
	}
	return 1, nil
}

// buildRoutineContext freezes the statement-visible values at the physical
// execution boundary. The plan carries only immutable routine metadata; it
// must not carry a statement identity or mutable session values because a
// prepared plan can outlive both.
func buildRoutineContext(proc *process.Process) (map[string]string, error) {
	if proc == nil || proc.Base == nil {
		return nil, fmt.Errorf("python udf: process is unavailable for statement context")
	}
	context := make(map[string]string)
	// The current query id is stable for the statement and changes for every
	// prepared execution; NewInvocationTuple generates one only for internal
	// tests that have no query id. It is control metadata, never plan data.
	if queryID := proc.QueryId(); queryID != "" {
		context["statement_id"] = queryID
	} else {
		delete(context, "statement_id")
	}
	queryStart := proc.GetStmtProfile().GetQueryStart()
	if queryStart.IsZero() {
		// Internal/background processes do not always install a statement
		// profile. UnixTime is process-scoped and therefore deterministic for
		// the lifetime of that process; a real frontend statement always uses
		// its query start above.
		if proc.Base.UnixTime == 0 {
			return nil, fmt.Errorf("python udf: statement timestamp is unavailable")
		}
		queryStart = time.Unix(0, proc.Base.UnixTime)
	}
	context["statement_timestamp_utc"] = strconv.FormatInt(queryStart.UTC().UnixMicro(), 10)

	location := proc.GetSessionInfo().TimeZone
	if location == nil {
		return nil, fmt.Errorf("python udf: session timezone is unavailable")
	}
	zoneName := location.String()
	var timezoneErr error
	if zoneName == "" || zoneName == "SYSTEM" {
		return nil, fmt.Errorf("python udf: session timezone is not a stable IANA identity")
	}
	if zoneName == "Local" {
		zoneName, location, timezoneErr = resolveSystemTimezone()
		if timezoneErr != nil {
			return nil, timezoneErr
		}
	}
	if zoneName == "FixedZone" {
		_, offset := queryStart.In(location).Zone()
		context["session_timezone_kind"] = "FIXED_OFFSET"
		context["session_timezone_offset_minutes"] = formatTimezoneOffsetMinutes(offset / 60)
		delete(context, "session_timezone_name")
		delete(context, "session_timezone_tzdb_version")
	} else {
		context["session_timezone_kind"] = "IANA"
		context["session_timezone_name"] = zoneName
		tzdbVersion, err := udf.TimezoneDatabaseVersion()
		if err != nil {
			return nil, err
		}
		context["session_timezone_tzdb_version"] = tzdbVersion
		delete(context, "session_timezone_offset_minutes")
	}

	mode := sessionSQLMode(proc)
	encodedMode, err := json.Marshal(mode)
	if err != nil {
		return nil, fmt.Errorf("python udf: encode sql_mode context: %w", err)
	}
	context["sql_mode"] = string(encodedMode)
	info := proc.GetSessionInfo()
	context["current_user"] = info.User
	context["connection_collation"] = info.GetCollation()
	if info.Database != "" {
		context["current_database"] = info.Database
	} else {
		delete(context, "current_database")
	}
	if info.Role != "" {
		context["current_role"] = info.Role
	} else {
		delete(context, "current_role")
	}
	return context, nil
}

// resolveSystemTimezone turns the frontend's SYSTEM location into the IANA
// identity that is part of the Python call contract. time.Local intentionally
// hides that identity behind the name "Local"; accepting that name would make
// two workers interpret a DST boundary from different host configuration.
// Prefer an explicit TZ setting, then use the conventional /etc/localtime
// symlink. A regular localtime file has no portable IANA identity and fails
// closed until the deployment supplies one.
func resolveSystemTimezone() (string, *time.Location, error) {
	candidates := []string{strings.TrimSpace(os.Getenv("TZ"))}
	if link, err := os.Readlink("/etc/localtime"); err == nil {
		const marker = "/zoneinfo/"
		if index := strings.Index(link, marker); index >= 0 {
			candidates = append(candidates, link[index+len(marker):])
		}
	}
	for _, candidate := range candidates {
		candidate = strings.TrimPrefix(candidate, ":")
		candidate = strings.TrimPrefix(candidate, "posix/")
		candidate = strings.TrimPrefix(candidate, "right/")
		if candidate == "" || candidate == "Local" || candidate == "SYSTEM" || strings.HasPrefix(candidate, "/") || strings.Contains(candidate, "..") {
			continue
		}
		location, err := time.LoadLocation(candidate)
		if err == nil {
			return candidate, location, nil
		}
	}
	return "", nil, fmt.Errorf("python udf: SYSTEM timezone has no stable IANA identity")
}

func formatTimezoneOffsetMinutes(offset int) string {
	if offset >= 0 {
		return "+" + strconv.Itoa(offset)
	}
	return strconv.Itoa(offset)
}

func sessionSQLMode(proc *process.Process) []string {
	value := ""
	if proc != nil && proc.GetSessionInfo() != nil {
		value = proc.GetSessionInfo().SqlMode
	}
	if proc != nil {
		if resolve := proc.GetResolveVariableFunc(); resolve != nil {
			if resolved, err := resolve("sql_mode", true, false); err == nil {
				if text, ok := resolved.(string); ok {
					value = text
				}
			}
		}
	}
	if value == process.EmptySqlModeSentinel {
		value = ""
	}
	seen := make(map[string]struct{})
	for _, item := range strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r' }) {
		item = strings.ToUpper(strings.TrimSpace(item))
		if item != "" {
			seen[item] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for item := range seen {
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}
