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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSONReaderMemoryAdmission binds metadata and cache pins to this execution
// generation. It captures the context and generation rather than a reusable
// Process, so a retained handle cannot silently charge a later query.
func JSONReaderMemoryAdmission(proc *process.Process, contexts ...context.Context) (vector.JSONReaderMemoryAdmission, error) {
	if proc == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader requires an execution context")
	}
	ctx := proc.Ctx
	if len(contexts) != 0 && contexts[0] != nil {
		ctx = contexts[0]
	}
	if ctx == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader requires an execution context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	generation, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	return func(capacity uint64) (func(), error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reservation, err := generation.ReserveTransientMemory(capacity)
		if err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			reservation.Release()
			return nil, err
		}
		return func() { reservation.Release() }, nil
	}, nil
}

// JSONReaderMemoryAdmissionForEvaluation preserves JSON_VALUE's existing
// planning/folding context choice while binding the resulting generation.
func JSONReaderMemoryAdmissionForEvaluation(proc *process.Process) (vector.JSONReaderMemoryAdmission, error) {
	return JSONReaderMemoryAdmission(proc, jsonValueEvaluationContext(proc))
}

func newJSONReaderBuilderForEvaluation(proc *process.Process, allocation *vector.AllocationAccountSelection) (*vector.JSONReaderBuilder, error) {
	admission, err := JSONReaderMemoryAdmissionForEvaluation(proc)
	if err != nil {
		return nil, err
	}
	return vector.NewJSONReaderBuilder(proc.Mp(), allocation, admission)
}
