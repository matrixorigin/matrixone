// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package function

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		if !pythonTypesEqual(receivedArgs[i], requiredArgs[i]) {
			canCast := false
			if receivedArgs[i].Oid == requiredArgs[i].Oid {
				canCast = pythonSameOIDCanNormalize(receivedArgs[i], requiredArgs[i])
			} else {
				canCast, _ = fixedImplicitTypeCast(receivedArgs[i], requiredArgs[i].Oid)
			}
			if canCast && receivedArgs[i].Oid == requiredArgs[i].Oid {
				// SQL's cast executor is also the type normalizer for Python's
				// frozen descriptor.  In particular, DECIMAL scale and precision
				// must be normalized before Arrow encoding; comparing only OID
				// would reinterpret the same coefficient at the wrong scale.
				canCast = true
			}
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

func pythonTypesEqual(left, right types.Type) bool {
	// Type.Eq applies SQL value semantics. In particular, integer display
	// width and the legacy scale marker are not part of the value contract;
	// CASE and table expressions are allowed to materialize those fields
	// differently while retaining the same fixed-width Arrow type. Decimal,
	// string, temporal, and vector metadata remain part of the comparison.
	return left.Eq(right)
}

// PythonUdfArgTypeMatch applies the ordinary implicit cast graph while also
// treating the complete frozen descriptor as the exact-match key. The shared
// UDF resolver historically compared only OIDs, which made two Python
// overloads such as DECIMAL(18,2) and DECIMAL(18,6) indistinguishable and
// allowed a non-exact candidate to win with the same cost as an exact one.
func PythonUdfArgTypeMatch(from, to []types.Type) (bool, int) {
	if len(from) != len(to) {
		return false, -1
	}
	length := len(from)
	cost := 0
	for index := range from {
		if pythonTypesEqual(from[index], to[index]) {
			continue
		}
		if pythonSameOIDCanNormalize(from[index], to[index]) {
			// A same-OID descriptor change is a SQL cast to the declared
			// width/scale. It is valid, but less specific than an exact match.
			cost++
			continue
		}
		if from[index].Oid == to[index].Oid {
			return false, -1
		}
		canCast, castCost := fixedImplicitTypeCast(from[index], to[index].Oid)
		if !canCast {
			return false, -1
		}
		if castCost == 1 {
			cost += castCost
		} else {
			cost += castCost * length
		}
	}
	return true, cost
}

// pythonSameOIDCanNormalize reports whether a descriptor change with the same
// SQL OID has a meaningful SQL cast. Fixed-size vector dimensions are part of
// the value shape: the existing array->array cast preserves bytes when the
// element OID is unchanged, so accepting VECF32(4) for VECF32(3) would publish
// a value with a descriptor that lies about its child count. Other supported
// same-OID metadata (decimal precision/scale, text width/charset, and temporal
// scale) is normalized by the ordinary SQL cast executor.
func pythonSameOIDCanNormalize(from, to types.Type) bool {
	if from.Oid != to.Oid || pythonTypesEqual(from, to) {
		return false
	}
	switch from.Oid {
	case types.T_array_float32, types.T_array_float64:
		return false
	default:
		return true
	}
}

// PythonUdfArgTypeCast returns the exact descriptor that the Python handler
// declared for every argument.  The generic UDF cast helper only receives an
// OID list, so it intentionally preserves a same-OID source type.  That is
// unsafe for Python's frozen contract: DECIMAL(18,2) and DECIMAL(18,6) have
// the same OID but different Arrow interpretation.  The SQL cast expression
// must therefore materialize the target descriptor even when only metadata
// differs.
func PythonUdfArgTypeCast(from, to []types.Type) []types.Type {
	if len(from) != len(to) {
		return nil
	}
	castTypes := make([]types.Type, len(from))
	for index := range from {
		if pythonTypesEqual(from[index], to[index]) {
			castTypes[index] = from[index]
			continue
		}
		if from[index].Oid == to[index].Oid && !pythonSameOIDCanNormalize(from[index], to[index]) {
			return nil
		}
		castTypes[index] = to[index]
	}
	return castTypes
}

// param parameters is same with param inputs in function checkPythonUdf
func pythonUdfRetType(parameters []types.Type) types.Type {
	return parameters[len(parameters)-1]
}

// rejectPythonJSONPlan is kept as the explicit stale-plan boundary for the
// historical overload id. Long-term execution enters through the typed
// RoutineCall field and ExternalRoutineEval.
func rejectPythonJSONPlan(_ []*vector.Vector, _ vector.FunctionResultWrapper, _ *process.Process, _ int, _ *FunctionSelectList) error {
	return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python JSON plan execution is not supported; reprepare the statement")
}

func validatePythonRoutineDescriptor(descriptor *vector.Vector) error {
	if descriptor == nil {
		return fmt.Errorf("python udf: missing routine descriptor")
	}
	if !descriptor.IsConst() {
		return fmt.Errorf("python udf: routine descriptor must be constant")
	}
	if descriptor.Length() == 0 {
		return fmt.Errorf("python udf: routine descriptor is empty")
	}
	return nil
}

func validatePythonInputVectors(inputs []*vector.Vector, args []types.Type, length int) error {
	if len(inputs) != len(args) {
		return fmt.Errorf("python udf: input column count %d does not match routine argument count %d", len(inputs), len(args))
	}
	for index, input := range inputs {
		if input == nil {
			return fmt.Errorf("python udf: input vector %d is nil", index)
		}
		if input.Length() == 0 {
			return fmt.Errorf("python udf: input vector %d is empty", index)
		}
		if !input.IsConst() && input.Length() < length {
			return fmt.Errorf("python udf: input vector %d is shorter than invocation length", index)
		}
		if input.GetType() == nil || !pythonTypesEqual(*input.GetType(), args[index]) {
			actual := "<nil>"
			if input.GetType() != nil {
				actual = input.GetType().String()
			}
			return fmt.Errorf("python udf: input vector %d type %s does not match %s", index, actual, args[index].String())
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
	invocationID, err := uuid.NewV7()
	if err != nil {
		return protocol.FencingTuple{}, fmt.Errorf("python udf: create invocation fence: %w", err)
	}
	groupID := context["group_id"]
	if groupID == "" {
		// The current physical adapter owns one group per invocation. A stable
		// statement-only group would reject the second batch or second routine
		// in the same statement; the scheduler may supply a shared group and
		// explicit epoch when it is introduced.
		groupID = statementID + "/python/" + invocationID.String()
	} else if context["group_epoch"] == "" {
		return protocol.FencingTuple{}, fmt.Errorf("python udf: explicit group_id requires group_epoch")
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

// NewInvocationTuple is shared by the physical external evaluator and protocol
// tests. A prepared plan supplies no execution identity; the caller provides
// the current query id and this helper creates a fresh invocation fence.
func NewInvocationTuple(context map[string]string, queryID string, accountID uint32) (protocol.FencingTuple, error) {
	return invocationTuple(context, queryID, accountID)
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
