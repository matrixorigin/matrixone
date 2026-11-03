//go:build integration

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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BuildPreparedForIntegrationTest preserves the existing parameter-domain
// binder while using the same S3 request owner as SELECT/PREPARE. The isolated
// binding process borrows the statement's immutable transport only during bind;
// neither Close nor the returned plan owns the parent's parameter vector.
func (r *ViewSchemaRequest) BuildPreparedForIntegrationTest(stmt tree.Statement, bindings []PreparedSourceBinding, values []any, parent *process.Process) (*PreparedExecutionPlan, error) {
	var bound *PreparedExecutionPlan
	_, err := r.buildPlan(stmt, true, func(compiler CompilerContext) (*Plan, error) {
		proc := compiler.GetProcess()
		if proc == nil || parent == nil || proc == parent {
			return nil, moerr.NewInternalError(r.ctx, "prepared View binding requires an isolated process")
		}
		params := parent.GetPrepareParams()
		if params != nil {
			n := params.Length()
			isBin, binary := make([]bool, n), make([]bool, n)
			kinds, paramTypes := make([]vector.PrepareParamKind, n), make([]types.T, n)
			for i := range n {
				isBin[i], binary[i] = parent.GetPrepareParamIsBin(i), parent.GetPrepareParamIsBinaryString(i)
				kinds[i], paramTypes[i] = parent.GetPrepareParamKind(i), parent.GetPrepareParamType(i)
			}
			proc.SetPrepareParamsWithTypedMeta(params, isBin, kinds, paramTypes, binary)
			defer proc.SetPrepareParams(nil)
		}
		var buildErr error
		bound, buildErr = BuildPreparedExecutionPlan(compiler, stmt, bindings, values)
		if buildErr != nil {
			return nil, buildErr
		}
		return bound.Plan, nil
	})
	if err != nil {
		return nil, err
	}
	return bound, nil
}

// DisableMemoForIntegrationTest selects the cache-off oracle for a newly
// constructed metadata request. Call only before publishing or using the request;
// this test seam is not compiled into ordinary production builds.
func (r *ViewMetadataRequest) DisableMemoForIntegrationTest() {
	r.schema.memoDisabled = true
}
