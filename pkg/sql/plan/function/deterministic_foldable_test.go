// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestGetFunctionIsDeterministicFoldableByIdRejectsEveryInternalClass pins the
// class half of the deterministic-fold admission. Function classes are bit
// flags, and a registry row may combine plan.Function_INTERNAL with other
// flags: python_user_defined_function is INTERNAL|STRICT and its overloads
// carry neither the volatile nor the real-time-related flag, so both fold
// recursion predicates pass for it and only the class test excludes it. The
// exclusion must therefore test the INTERNAL bit, never compare the whole
// class word: a combined registration that slips through would let the
// publication binding probe's guarded fold execute the internal machinery -
// for the python UDF builtin, right into UdfService.Run.
func TestGetFunctionIsDeterministicFoldableByIdRejectsEveryInternalClass(t *testing.T) {
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
				GetFunctionIsDeterministicFoldableById(encodeOverloadID(int32(fn.functionId), int32(ovIndex))),
				"function id %d (class word %d) carries the INTERNAL bit and must never qualify as a deterministic admission for overload %d",
				fn.functionId, uint32(fn.class), ovIndex)
		}
	}
	require.NotZero(t, internalRegistrations,
		"the registry still registers internal machinery the exclusion must cover")
	require.NotZero(t, combinedInternalRegistrations,
		"the registry still combines the INTERNAL flag with other class flags, which an equality check would miss")

	// The finding's own example: the python UDF machinery is registered with
	// a combined class and no volatile flag, so the class bit is the only
	// gate that keeps the guarded fold away from the UDF service.
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

	// The pure deterministic admission keeps working: abs of a literal is the
	// shape the publication probe must fold like ordinary binding does.
	absFid, exists := getFunctionIdByNameWithoutErr("abs")
	require.True(t, exists)
	absAdmissible := false
	for ovIndex := range allSupportedFunctions[absFid].Overloads {
		if GetFunctionIsDeterministicFoldableById(encodeOverloadID(absFid, int32(ovIndex))) {
			absAdmissible = true
		}
	}
	require.True(t, absAdmissible,
		"the pure deterministic admission still admits the abs builtin")
}
