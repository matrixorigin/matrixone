// Copyright 2021 - 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestGetFunctionIsProvenPureFoldableByIdRejectsEveryInternalClass pins the
// class half of the guarded-fold admission. Function classes are bit
// flags, and a registry row may combine plan.Function_INTERNAL with other
// flags: python_user_defined_function is INTERNAL|STRICT and its overloads
// carry neither the volatile nor the real-time-related flag, so the
// exclusion must test the INTERNAL bit, never compare the whole class
// word: a combined registration that slips through would let the
// publication binding probe's guarded fold execute the internal machinery -
// for the python UDF builtin, right into UdfService.Run.
func TestGetFunctionIsProvenPureFoldableByIdRejectsEveryInternalClass(t *testing.T) {
	internalRegistrations := 0
	combinedInternalRegistrations := 0
	for fid := range allSupportedFunctions {
		fn := allSupportedFunctions[fid]
		if !fn.testFlag(plan.Function_INTERNAL) {
			continue
		}
		internalRegistrations++
		if fn.class != plan.Function_INTERNAL {
			combinedInternalRegistrations++
		}
		for ovIndex := range fn.Overloads {
			require.False(t,
				GetFunctionIsProvenPureFoldableById(encodeOverloadID(int32(fn.functionId), int32(ovIndex))),
				"function id %d (class word %d) carries the INTERNAL bit and must never qualify as a proven pure admission for overload %d",
				fn.functionId, uint32(fn.class), ovIndex)
		}
	}
	require.NotZero(t, internalRegistrations,
		"the registry still registers internal machinery the exclusion must cover")
	require.NotZero(t, combinedInternalRegistrations,
		"the registry still combines the INTERNAL flag with other class flags, which an equality check would miss")

	// A registration that is positively proven pure is never registered as
	// internal machinery: membership and the class bit are independent
	// gates, and the curated set must never grow an internal entry.
	for name := range provenPureValueFunctions {
		fid, exists := getFunctionIdByNameWithoutErr(name)
		require.True(t, exists, "every curated pure name resolves to a registration")
		require.False(t, allSupportedFunctions[fid].testFlag(plan.Function_INTERNAL),
			"curated pure builtin %q carries the internal-machinery class bit", name)
	}

	// The finding's own example: the python UDF machinery is registered with
	// a combined class and no volatile flag, so the class bit (together
	// with pure-set membership) keeps the guarded fold away from the UDF
	// service.
	pythonUdfFid, exists := getFunctionIdByNameWithoutErr("python_user_defined_function")
	require.True(t, exists, "python_user_defined_function is a registered builtin")
	require.True(t, allSupportedFunctions[pythonUdfFid].testFlag(plan.Function_INTERNAL),
		"the python UDF machinery carries the INTERNAL flag")
	require.NotEqual(t, plan.Function_INTERNAL, allSupportedFunctions[pythonUdfFid].class,
		"the python UDF machinery combines INTERNAL with other class flags")
	for ovIndex := range allSupportedFunctions[pythonUdfFid].Overloads {
		require.False(t, allSupportedFunctions[pythonUdfFid].Overloads[ovIndex].CannotFold(),
			"python UDF overload %d carries no volatile flag, so only the class bit excludes it", ovIndex)
	}
}

// TestGetFunctionIsProvenPureFoldableByIdAdmitsOnlyPositivelyProvenRegistrations
// pins the membership half of the guarded-fold admission: the admission is
// a closed set. Every overload of every resolved registration passes the
// predicate exactly when its function is in the curated pure-value list and
// carries none of the excluded flags, so an unlisted registration - however
// clean its flags - stays rejected. The negative flags alone establish
// nothing: the registry holds them for functions whose executors still
// reach outside the expression tree.
func TestGetFunctionIsProvenPureFoldableByIdAdmitsOnlyPositivelyProvenRegistrations(t *testing.T) {
	// Every curated name resolved at package init, and each is in the
	// resolved id view. Several names alias one registration (pow/power,
	// substr/substring/mid, ...), so the id view is a deduplicated view
	// and may be smaller than the name list.
	for name := range provenPureValueFunctions {
		fid, ok := functionIdRegister[name]
		require.True(t, ok, "curated pure builtin %q is registered", name)
		_, member := provenPureValueFunctionIds[fid]
		require.True(t, member, "curated pure builtin %q is in the resolved id view", name)
	}

	// The pure deterministic positive keeps working: abs of a literal is
	// the shape the publication probe must fold like ordinary binding does.
	absFid, exists := getFunctionIdByNameWithoutErr("abs")
	require.True(t, exists, "abs is a registered builtin")
	absAdmissible := false
	for ovIndex := range allSupportedFunctions[absFid].Overloads {
		if GetFunctionIsProvenPureFoldableById(encodeOverloadID(absFid, int32(ovIndex))) {
			absAdmissible = true
		}
	}
	require.True(t, absAdmissible,
		"the curated pure set still admits the abs builtin")

	for fid := range allSupportedFunctions {
		fn := allSupportedFunctions[fid]
		_, proven := provenPureValueFunctionIds[int32(fid)]
		for ovIndex := range fn.Overloads {
			encoded := encodeOverloadID(int32(fid), int32(ovIndex))
			flagClean := !fn.testFlag(plan.Function_INTERNAL) &&
				!fn.Overloads[ovIndex].CannotFold() &&
				!fn.Overloads[ovIndex].IsRealTimeRelated()
			require.Equal(t, proven && flagClean,
				GetFunctionIsProvenPureFoldableById(encoded),
				"admission for function id %d overload %d must equal curated membership and clean flags, not any other rule",
				fn.functionId, ovIndex)
		}
	}

	require.False(t, GetFunctionIsProvenPureFoldableById(-77),
		"an unresolvable overload id never qualifies")
}

// TestGetFunctionIsProvenPureFoldableByIdRejectsTheReviewWitnesses pins the
// mo-bot-local review's counterexample class: registrations that carry none
// of the excluded flags - no internal class bit, no volatile overload, no
// real-time-related overload - and still must not be admitted, because
// their executors reach outside the expression tree. llm_chat and
// llm_embedding perform requests through the LLM service machinery, and
// try_starlark executes starlark code; a negative-only admission gate
// passed all three during metadata-only publication validation.
func TestGetFunctionIsProvenPureFoldableByIdRejectsTheReviewWitnesses(t *testing.T) {
	witnesses := []string{"llm_chat", "llm_embedding", "try_starlark"}
	for _, name := range witnesses {
		fid, exists := getFunctionIdByNameWithoutErr(name)
		require.True(t, exists, "%s is a registered builtin", name)
		fn := allSupportedFunctions[fid]
		require.False(t, fn.testFlag(plan.Function_INTERNAL),
			"%s carries no internal-machinery class bit, which is exactly the finding: its flags alone do not exclude it", name)
		for ovIndex := range fn.Overloads {
			require.False(t, fn.Overloads[ovIndex].CannotFold(),
				"%s overload %d carries no volatile flag", name, ovIndex)
			require.False(t, fn.Overloads[ovIndex].IsRealTimeRelated(),
				"%s overload %d carries no real-time-related flag", name, ovIndex)
			require.False(t,
				GetFunctionIsProvenPureFoldableById(encodeOverloadID(int32(fn.functionId), int32(ovIndex))),
				"%s is not a positively proven pure value transformation, so the guarded publication fold must never admit it (overload %d)",
				name, ovIndex)
		}
	}
}
