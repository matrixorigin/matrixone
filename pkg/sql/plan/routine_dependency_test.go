// Copyright 2026 Matrix Origin
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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/stretchr/testify/require"
)

func testRoutineCallForDependency() *planpb.RoutineCall {
	return &planpb.RoutineCall{
		ContractVersion: udf.PythonPlanContractVersion,
		FunctionRef: &planpb.FunctionRef{
			FunctionId:       11,
			Revision:         3,
			NamespaceVersion: 4,
			AccountId:        5,
			DatabaseId:       6,
		},
		Language:     udf.LanguagePython,
		Volatility:   "VOLATILE",
		NullPolicy:   udf.NullCallHandler,
		MayError:     true,
		SecurityMode: "INVOKER",
		Leakproof:    false,
		Implementation: &planpb.RoutineCall_Python{Python: &planpb.PythonRoutineImplementation{
			DefinitionFingerprint: "fingerprint",
			ArtifactDigest:        "artifact",
			EnvironmentDigest:     "environment",
		}},
	}
}

func TestRecordRoutinePlanDependencyDeduplicatesAndCopiesIdentity(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{}}
	call := testRoutineCallForDependency()

	require.NoError(t, builder.recordRoutinePlanDependency(call))
	require.NoError(t, builder.recordRoutinePlanDependency(call))
	require.Len(t, builder.qry.RoutineDependencies, 1)

	dependency := builder.qry.RoutineDependencies[0]
	require.Equal(t, uint64(11), dependency.FunctionRef.FunctionId)
	require.Equal(t, uint64(3), dependency.FunctionRef.Revision)
	require.Equal(t, "fingerprint", dependency.DefinitionFingerprint)
	require.Equal(t, "artifact", dependency.ArtifactDigest)
	require.Equal(t, "environment", dependency.EnvironmentDigest)
	require.Equal(t, "VOLATILE", dependency.Volatility)
	require.Equal(t, udf.NullCallHandler, dependency.NullPolicy)
	require.True(t, dependency.MayError)
	require.Equal(t, "INVOKER", dependency.SecurityMode)
	require.False(t, dependency.Leakproof)

	// The envelope is a plan-owned snapshot of the call identity. Mutating the
	// source FunctionRef after binding must not mutate cache validation state.
	call.FunctionRef.Revision = 99
	require.Equal(t, uint64(3), dependency.FunctionRef.Revision)
}

func TestAssignRoutineCallsiteIsPlanLocalAndIdempotent(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{}}
	call := testRoutineCallForDependency()
	require.NoError(t, builder.assignRoutineCallsite(call))
	require.Equal(t, "python/1", call.CallsiteId)
	require.NoError(t, builder.assignRoutineCallsite(call))
	require.Equal(t, "python/1", call.CallsiteId)

	second := testRoutineCallForDependency()
	require.NoError(t, builder.assignRoutineCallsite(second))
	require.Equal(t, "python/2", second.CallsiteId)
}

func TestRoutineDependencyRejectsNoncanonicalLanguage(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{}}
	call := testRoutineCallForDependency()
	call.Language = "PYTHON"
	require.ErrorContains(t, builder.assignRoutineCallsite(call), "language")
	require.ErrorContains(t, builder.recordRoutinePlanDependency(call), "language")
}

func TestRecordRoutinePlanDependencyBoundsPlanClosure(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{
		RoutineDependencies: make([]*planpb.RoutinePlanDependency, maxRoutinePlanDependencies),
	}}

	err := builder.recordRoutinePlanDependency(testRoutineCallForDependency())
	require.ErrorContains(t, err, "PROGRAM_LIMIT_EXCEEDED")
}

func TestRecordSQLRoutinePlanDependencyUsesSharedEnvelope(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{}}
	call := &planpb.RoutineCall{
		ContractVersion: udf.RoutinePlanContractVersion,
		FunctionRef: &planpb.FunctionRef{
			FunctionId: 11, Revision: 3, NamespaceVersion: 4, AccountId: 5, DatabaseId: 6,
		},
		Language:     udf.LanguageSQL,
		Volatility:   "VOLATILE",
		NullPolicy:   udf.NullCallHandler,
		MayError:     true,
		SecurityMode: "DEFINER",
		Leakproof:    false,
		Implementation: &planpb.RoutineCall_Sql{Sql: &planpb.SqlRoutineImplementation{
			DefinitionFingerprint: []byte("0123456789abcdef"),
		}},
	}
	require.NoError(t, builder.recordRoutinePlanDependency(call))
	require.Len(t, builder.qry.RoutineDependencies, 1)
	require.Equal(t, udf.LanguageSQL, builder.qry.RoutineDependencies[0].Language)
	require.Equal(t, "0123456789abcdef", builder.qry.RoutineDependencies[0].DefinitionFingerprint)
	require.Empty(t, builder.qry.RoutineDependencies[0].ArtifactDigest)
	require.Empty(t, builder.qry.RoutineDependencies[0].EnvironmentDigest)
}
