// Copyright 2026 MatrixOne
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateLockingCurrentReadCapability is the D-8 layer-3 Run-admission
// validator (design rev 14, D-10.1): a plan whose LockTargets carry bind-time
// locking-read presence requires the live negotiated cluster capability
// (D-7, the ALLOCATED gate MORPCVersion135 — never the colliding upstream
// aisles 130–134, verified against origin/main c4946bb74d43f0f where
// MORPCLatestVersion = 134 already consumes 132–134 for decimal-float,
// JSON-MIN/MAX and ODKU features). A
// marked plan observed below the gate fails its execution with the
// NotSupported "requires rebinding" terminal error — the single recorded
// new-error exception — instead of executing mixed reads; the frontend
// protocolVersion rebuild machinery (layer 2) turns the ordinary session
// cases into a clean unmarked rebinding. Unmarked plans — the only kind the
// plan builder produces below the gate — always pass, so below-gate
// behavior is byte-identical to today.
func validateLockingCurrentReadCapability(proc *process.Process, owner any) error {
	if proc == nil {
		return moerr.NewInternalErrorNoCtx("locking current-read admission requires a process")
	}
	qry, _ := plan2.PlanLockingReadQuery(owner)
	if !plan2.PlanHasLockingCurrentRead(qry) {
		return nil
	}
	if plan2.ClusterHasLockingCurrentRead(proc) {
		return nil
	}
	version := int64(0)
	if rt := runtime.ServiceRuntime(proc.GetService()); rt != nil {
		if value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion); ok {
			if v, valid := value.(int64); valid {
				version = v
			}
		}
	}
	return moerr.NewNotSupportedf(proc.Ctx,
		"locking current-read execution requires rebinding (MORPC protocol version %d < MORPCVersion135)",
		version)
}
