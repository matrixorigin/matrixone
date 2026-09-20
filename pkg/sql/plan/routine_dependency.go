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
	"fmt"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

// maxRoutinePlanDependencies bounds the amount of catalog state retained by
// one executable plan. A plan with more distinct routine revisions is rejected
// during binding instead of creating an unbounded cache invalidation payload.
const maxRoutinePlanDependencies = 1024

// assignRoutineCallsite gives a bound routine expression a stable identity
// within its plan. It is not a function identity and is never used for
// catalog lookup; it lets the physical stage, diagnostics and future
// scheduler account for two references to the same revision independently.
func (builder *QueryBuilder) assignRoutineCallsite(call *planpb.RoutineCall) error {
	if builder == nil || call == nil {
		return nil
	}
	if call.Language != udf.LanguageSQL && call.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine call language %q is not canonical", call.Language)
	}
	if call.CallsiteId != "" {
		if len(call.CallsiteId) > 256 || strings.ContainsAny(call.CallsiteId, "\r\n") {
			return fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine callsite id is too large or invalid")
		}
		return nil
	}
	if builder.nextRoutineCallID == ^uint64(0) {
		return fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine callsite id exhausted")
	}
	builder.nextRoutineCallID++
	call.CallsiteId = fmt.Sprintf("%s/%d", strings.ToLower(call.Language), builder.nextRoutineCallID)
	return nil
}

func (builder *QueryBuilder) recordRoutinePlanDependency(call *planpb.RoutineCall) error {
	if builder == nil || builder.qry == nil || call == nil || call.FunctionRef == nil {
		return nil
	}
	if call.Language != udf.LanguageSQL && call.Language != udf.LanguagePython {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: routine call language %q is not canonical", call.Language)
	}
	ref := call.FunctionRef
	dependency := &planpb.RoutinePlanDependency{
		FunctionRef: &planpb.FunctionRef{
			FunctionId:       ref.FunctionId,
			Revision:         ref.Revision,
			NamespaceVersion: ref.NamespaceVersion,
			AccountId:        ref.AccountId,
			DatabaseId:       ref.DatabaseId,
		},
		Language:              call.Language,
		NamespaceFingerprint:  call.NamespaceFingerprint,
		ContractVersion:       call.ContractVersion,
		DefinitionFingerprint: "",
		Volatility:            call.Volatility,
		NullPolicy:            call.NullPolicy,
		MayError:              call.MayError,
		SecurityMode:          call.SecurityMode,
		Leakproof:             call.Leakproof,
	}
	if python := call.GetPython(); python != nil {
		dependency.DefinitionFingerprint = python.DefinitionFingerprint
		dependency.ArtifactDigest = python.ArtifactDigest
		dependency.EnvironmentDigest = python.EnvironmentDigest
	} else if sql := call.GetSql(); sql != nil {
		// SQL uses the same dependency envelope. The implementation digest is
		// stored as the canonical hexadecimal SHA-256 string so cache
		// validation has the same representation across languages.
		dependency.DefinitionFingerprint = string(sql.DefinitionFingerprint)
	}
	for _, existing := range builder.qry.RoutineDependencies {
		if routinePlanDependenciesEqual(existing, dependency) {
			return nil
		}
	}
	if len(builder.qry.RoutineDependencies) >= maxRoutinePlanDependencies {
		return fmt.Errorf("PROGRAM_LIMIT_EXCEEDED: routine dependency count exceeds %d", maxRoutinePlanDependencies)
	}
	builder.qry.RoutineDependencies = append(builder.qry.RoutineDependencies, dependency)
	return nil
}

func routinePlanDependenciesEqual(left, right *planpb.RoutinePlanDependency) bool {
	if left == nil || right == nil || left.FunctionRef == nil || right.FunctionRef == nil {
		return left == right
	}
	return left.NamespaceFingerprint == right.NamespaceFingerprint &&
		left.Language == right.Language &&
		left.ContractVersion == right.ContractVersion &&
		left.DefinitionFingerprint == right.DefinitionFingerprint &&
		left.ArtifactDigest == right.ArtifactDigest &&
		left.EnvironmentDigest == right.EnvironmentDigest &&
		left.Volatility == right.Volatility &&
		left.NullPolicy == right.NullPolicy &&
		left.MayError == right.MayError &&
		left.SecurityMode == right.SecurityMode &&
		left.Leakproof == right.Leakproof &&
		left.FunctionRef.FunctionId == right.FunctionRef.FunctionId &&
		left.FunctionRef.Revision == right.FunctionRef.Revision &&
		left.FunctionRef.NamespaceVersion == right.FunctionRef.NamespaceVersion &&
		left.FunctionRef.AccountId == right.FunctionRef.AccountId &&
		left.FunctionRef.DatabaseId == right.FunctionRef.DatabaseId
}
