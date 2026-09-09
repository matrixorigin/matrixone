// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package function

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// param inputs has four parts:
//  1. inputs[0]: udf, function self
//  2. inputs[1 : size+1]: receivedArgs, args which function received
//  3. inputs[size+1 : 2*size+1]: requiredArgs, args which function required
//  4. inputs[2*size+1]: ret, function ret
//     which size = (len(inputs) - 2) / 2
func checkPythonUdf(overloads []overload, inputs []types.Type) checkResult {

	if len(inputs)%2 == 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if len(inputs) == 2 {
		return newCheckResultWithSuccess(0)
	}
	size := (len(inputs) - 2) / 2
	receivedArgs := inputs[1 : size+1]
	requiredArgs := inputs[size+1 : 2*size+1]
	needCast := false
	for i := 0; i < size; i++ {
		if receivedArgs[i].Oid != requiredArgs[i].Oid {
			canCast, _ := fixedImplicitTypeCast(receivedArgs[i], requiredArgs[i].Oid)
			if !canCast {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			needCast = true
		}
	}
	if needCast {
		castType := make([]types.Type, size+2)
		castType[0] = inputs[0]
		for i, typ := range requiredArgs {
			castType[i+1] = typ
		}
		castType[size+1] = inputs[2*size+1]
		return newCheckResultWithCast(0, castType)
	}
	return newCheckResultWithSuccess(0)
}

// param parameters is same with param inputs in function checkPythonUdf
func pythonUdfRetType(parameters []types.Type) types.Type {
	return parameters[len(parameters)-1]
}

// param parameters has two parts:
//  1. parameters[0]: const vector udf
//  2. parameters[1:]: data vectors
//
// The SQL executor normally applies CASE/selection compaction before this
// function is called.  The NULL policy is still enforced here because it is
// part of the persisted Python routine contract and must not be inferred from
// the generic builtin STRICT bit.
func runPythonUdf(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) == 0 || parameters[0] == nil {
		return fmt.Errorf("python udf: missing routine descriptor")
	}
	if length < 0 {
		return fmt.Errorf("python udf: negative input length %d", length)
	}

	routine := &UdfWithContext{}
	encoded, isNull := vector.GenerateFunctionStrParameter(parameters[0]).GetStrValue(0)
	if isNull {
		return fmt.Errorf("python udf: routine descriptor is NULL")
	}
	if err := json.Unmarshal(encoded, routine); err != nil {
		return fmt.Errorf("python udf: decode routine descriptor: %w", err)
	}
	if routine.Udf == nil {
		return fmt.Errorf("python udf: routine descriptor has no function")
	}
	if err := routine.LoadPythonTypeContract(); err != nil {
		return fmt.Errorf("python udf: decode Python type contract: %w", err)
	}

	body := PythonRoutineBody{}
	if err := json.Unmarshal([]byte(routine.Body), &body); err != nil {
		return fmt.Errorf("python udf: decode Python routine body: %w", err)
	}
	if body.Handler == "" || body.Source == "" {
		return fmt.Errorf("python udf: routine handler and source are required")
	}
	if body.Mode != "SCALAR" && body.Mode != "VECTOR" {
		return fmt.Errorf("python udf: unsupported mode %q", body.Mode)
	}
	if body.NullPolicy != udf.NullCallHandler && body.NullPolicy != udf.NullReturnNull {
		return fmt.Errorf("python udf: unsupported NULL policy %q", body.NullPolicy)
	}
	if body.ABIContract != udf.PythonABIContract || body.AdapterVersion != udf.PythonAdapterVersion {
		return fmt.Errorf("python udf: unsupported Python ABI contract %q/%q", body.ABIContract, body.AdapterVersion)
	}
	if body.SDKVersion != udf.PythonSDKVersion {
		return fmt.Errorf("python udf: unsupported Python SDK %q", body.SDKVersion)
	}
	argTypes, err := routineArgumentTypes(routine)
	if err != nil {
		return err
	}
	if len(parameters)-1 != len(argTypes) {
		return fmt.Errorf("python udf: routine has %d arguments, received %d", len(argTypes), len(parameters)-1)
	}

	if length == 0 {
		return result.PreExtendAndReset(0)
	}
	for index, input := range parameters[1:] {
		if input == nil || (!input.IsConst() && input.Length() < length) {
			return fmt.Errorf("python udf: input vector %d is shorter than invocation length", index)
		}
	}
	// FunctionExpressionExecutor passes an empty, non-nil select list when the
	// caller selected every row.  In that state AnyNull is false and the
	// missing bitmap means "all rows", not a truncated selection.  A partial
	// selection must still carry one entry per invocation row so that a row
	// cannot accidentally be evaluated after CASE/short-circuit filtering.
	if selectList != nil && !selectList.ShouldEvalAllRow() && len(selectList.SelectList) < length {
		return fmt.Errorf("python udf: selection list is shorter than invocation length")
	}

	selected := make([]int64, 0, length)
	for row := 0; row < length; row++ {
		if selectList != nil && len(selectList.SelectList) > row && !selectList.SelectList[row] {
			continue
		}
		if body.NullPolicy == udf.NullReturnNull && hasNullInput(parameters[1:], row) {
			continue
		}
		selected = append(selected, int64(row))
	}
	if len(selected) == 0 {
		if err := result.PreExtendAndReset(length); err != nil {
			return err
		}
		result.GetResultVector().SetAllNulls(length)
		result.GetResultVector().SetLength(length)
		return nil
	}

	inputs := parameters[1:]
	var compacted []*vector.Vector
	if len(selected) != length {
		compacted = make([]*vector.Vector, len(inputs))
		for i, input := range inputs {
			if input == nil {
				return fmt.Errorf("python udf: input vector %d is nil", i)
			}
			compacted[i] = vector.NewOffHeapVecWithType(*input.GetType())
			compacted[i].SetIsBin(input.GetIsBin())
			if err := compacted[i].Union(input, selected, proc.Mp()); err != nil {
				freeVectors(compacted, proc.Mp())
				return fmt.Errorf("python udf: compact input %d: %w", i, err)
			}
		}
		inputs = compacted
		defer freeVectors(compacted, proc.Mp())
	}

	callResult := result
	var temporary vector.FunctionResultWrapper
	if len(selected) != length {
		temporary = vector.NewFunctionResultWrapper(routine.GetRetType(), proc.Mp())
		callResult = temporary
		defer temporary.Free()
	}

	accountID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return fmt.Errorf("python udf: resolve account: %w", err)
	}
	tuple, err := invocationTuple(routine.Context, proc.QueryId(), accountID)
	if err != nil {
		return err
	}
	invocation := &udf.Invocation{
		Language:       udf.LanguagePython,
		Handler:        body.Handler,
		Source:         body.Source,
		Args:           argTypes,
		ReturnType:     routine.GetRetType(),
		Inputs:         inputs,
		Length:         len(selected),
		Mode:           body.Mode,
		NullPolicy:     body.NullPolicy,
		ABIContract:    body.ABIContract,
		AdapterVersion: body.AdapterVersion,
		SDKVersion:     body.SDKVersion,
		Context:        cloneContext(routine.Context),
		Tuple:          tuple,
	}
	if err := proc.Base.UdfService.Execute(proc.Ctx, invocation, callResult, proc.Mp()); err != nil {
		return err
	}

	if len(selected) == length {
		return nil
	}
	if err := result.PreExtendAndReset(length); err != nil {
		return err
	}
	full := result.GetResultVector()
	full.ResetWithSameType()
	nullResult := vector.NewConstNull(routine.GetRetType(), 1, proc.Mp())
	defer nullResult.Free(proc.Mp())
	selectedRow := int64(0)
	for row := 0; row < length; row++ {
		if selectedRow < int64(len(selected)) && selected[selectedRow] == int64(row) {
			if err := full.UnionOne(callResult.GetResultVector(), selectedRow, proc.Mp()); err != nil {
				return err
			}
			selectedRow++
			continue
		}
		if err := full.UnionOne(nullResult, 0, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func hasNullInput(inputs []*vector.Vector, row int) bool {
	for _, input := range inputs {
		if input == nil || input.IsNull(uint64(row)) {
			return true
		}
	}
	return false
}

func routineArgumentTypes(routine *UdfWithContext) ([]types.Type, error) {
	if len(routine.PythonArgTypes) != 0 {
		args := make([]types.Type, len(routine.PythonArgTypes))
		for i, descriptor := range routine.PythonArgTypes {
			args[i] = descriptor.Type()
		}
		return args, nil
	}
	if len(routine.ArgsType) != 0 || len(routine.Args) == 0 {
		return append([]types.Type(nil), routine.ArgsType...), nil
	}
	args := make([]types.Type, len(routine.Args))
	for i, arg := range routine.Args {
		if arg == nil {
			return nil, fmt.Errorf("python udf: routine argument %d is nil", i)
		}
		typ, ok := types.Types[arg.Type]
		if !ok {
			return nil, fmt.Errorf("python udf: unknown routine argument type %q", arg.Type)
		}
		args[i] = typ.ToType()
	}
	return args, nil
}

func cloneContext(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func freeVectors(vectors []*vector.Vector, mp *mpool.MPool) {
	for _, vector := range vectors {
		if vector != nil {
			vector.Free(mp)
		}
	}
}

func invocationTuple(context map[string]string, queryID string, accountID uint32) (protocol.FencingTuple, error) {
	statementID := context["statement_id"]
	if statementID == "" {
		statementID = queryID
	}
	if statementID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return protocol.FencingTuple{}, fmt.Errorf("python udf: create statement fence: %w", err)
		}
		statementID = id.String()
	}
	groupID := context["group_id"]
	if groupID == "" {
		groupID = statementID + "/python"
	}
	invocationID, err := uuid.NewV7()
	if err != nil {
		return protocol.FencingTuple{}, fmt.Errorf("python udf: create invocation fence: %w", err)
	}
	groupEpoch, err := positiveContextUint(context, "group_epoch", 1)
	if err != nil {
		return protocol.FencingTuple{}, err
	}
	leaseEpoch, err := positiveContextUint(context, "lease_epoch", 1)
	if err != nil {
		return protocol.FencingTuple{}, err
	}
	return protocol.FencingTuple{
		AccountID:    uint64(accountID),
		StatementID:  statementID,
		GroupID:      groupID,
		GroupEpoch:   groupEpoch,
		InvocationID: invocationID.String(),
		LeaseEpoch:   leaseEpoch,
	}, nil
}

func positiveContextUint(context map[string]string, key string, fallback uint64) (uint64, error) {
	value := context[key]
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("python udf: invalid %s %q", key, value)
	}
	return parsed, nil
}
