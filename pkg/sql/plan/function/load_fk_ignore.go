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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// RequireLoadFKIgnoreProtocol fences binding and native executor construction.
// Observe the current rollout floor rather than retaining a binding-time value.
func RequireLoadFKIgnoreProtocol(ctx context.Context, proc *process.Process) error {
	if proc != nil {
		if rt := runtime.ServiceRuntime(proc.GetService()); rt != nil {
			value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
			version, valid := value.(int64)
			if ok && valid && version >= defines.MORPCVersion125 {
				return nil
			}
		}
	}
	return moerr.NewNotSupported(ctx, "LOAD IGNORE foreign-key filtering requires MORPC protocol version 125")
}

// loadFKIgnoreCheck is emitted only at explicit LOAD IGNORE's admission barrier.
// Parent existence and MATCH SIMPLE are resolved before this predicate. One
// combined FK result yields exactly one warning per rejected candidate.
func loadFKIgnoreCheck(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	flags := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	res := vector.MustFunctionResult[bool](result)
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := res.Append(true, true); err != nil {
				return err
			}
			continue
		}
		flag, isNull := flags.GetValue(i)
		pass := !isNull && flag
		if !pass {
			warnings.Add(moerr.ER_NO_REFERENCED_ROW_2, "Cannot add or update a child row: a foreign key constraint fails")
		}
		if err := res.Append(pass, false); err != nil {
			return err
		}
	}
	warnings.Flush(proc)
	return nil
}
