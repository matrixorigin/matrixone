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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// closePipelineSpoolsAfterReadersStopped runs only at the joined attempt/RPC
// boundary. In particular, an inactive prepared Compile's Release must not
// collect another attempt's spools merely because it shares a BaseProcess.
func (c *Compile) closePipelineSpoolsAfterReadersStopped() {
	owned := func(proc *process.Process) bool {
		if proc == nil || proc.Base == nil {
			return false
		}
		if c.importMemory != nil {
			_, ok := c.importMemoryBases[proc.Base]
			return ok
		}
		return proc.Base.ImportMemory == nil
	}
	if c.importMemory != nil {
		for base := range c.importMemoryBases {
			base.ReleasePipelineCurrentsAfterReadersStopped()
		}
	} else {
		releaseProc := func(proc *process.Process) {
			if owned(proc) {
				proc.Base.ReleasePipelineCurrentsAfterReadersStopped()
			}
		}
		releaseProc(c.proc)
		var release func(*Scope)
		release = func(scope *Scope) {
			if scope == nil {
				return
			}
			releaseProc(scope.Proc)
			for _, child := range scope.PreScopes {
				release(child)
			}
		}
		for _, scope := range c.scopes {
			release(scope)
		}
	}
	drainProc := func(proc *process.Process) {
		if owned(proc) {
			for _, reg := range proc.Reg.MergeReceivers {
				reg.DrainForCleanup()
			}
		}
	}
	drainProc(c.proc)
	var drain func(*Scope)
	drain = func(scope *Scope) {
		if scope == nil {
			return
		}
		if owned(scope.Proc) {
			drainProc(scope.Proc)
			_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
				switch value := op.(type) {
				case *connector.Connector:
					value.Reg.DrainForCleanup()
				case *dispatch.Dispatch:
					for _, reg := range value.LocalRegs {
						reg.DrainForCleanup()
					}
				}
				return nil
			})
		}
		for _, child := range scope.PreScopes {
			drain(child)
		}
	}
	for _, scope := range c.scopes {
		drain(scope)
	}
	if c.importMemory != nil {
		for base := range c.importMemoryBases {
			base.ClosePipelineSpoolsAfterReadersStopped()
		}
		return
	}
	closeBase := func(proc *process.Process) {
		if proc == nil || proc.Base == nil || proc.Base.ImportMemory != nil {
			return
		}
		proc.Base.ClosePipelineSpoolsAfterReadersStopped()
	}
	closeBase(c.proc)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil {
			return
		}
		closeBase(scope.Proc)
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}
