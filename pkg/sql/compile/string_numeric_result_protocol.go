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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// constrainStringNumericResultWorkers keeps corrected string numeric result
// expressions on one CN while a rolling cluster still contains workers below
// MORPC v80. The result wrapper and persisted numeric schema are part of the
// serialized expression contract, so every selected remote worker must agree.
func (c *Compile) constrainStringNumericResultWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !features.StringNumericResultContracts {
		return err
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion80)
	if err != nil {
		return err
	}
	if supported {
		return nil
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

// constrainStrictStringNumericCompatibilityWorkers keeps expressions whose
// conversion semantics changed in v103 away from older workers. Historical
// CEIL/FLOOR overloads require this in every mode. A legacy process marker is
// rejected rather than silently treated as the new sender contract.
func (c *Compile) constrainStrictStringNumericCompatibilityWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !requiresStringNumericCompatibilityProtocol(c.proc, features) {
		return err
	}
	if c.proc.GetSessionInfo().LegacyNumericCompatibilityMode {
		return moerr.NewNotSupportedNoCtx(
			"string numeric compatibility cannot run with a legacy session contract",
		)
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion103)
	if err != nil {
		return err
	}
	if supported {
		return nil
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

// validateStrictStringNumericCompatibilityDestination rechecks the worker
// capability at the final destination probe before the scope is serialized.
// A downgrade visible to this probe is rejected; the query-service probe and
// pipeline send do not bind a process generation, so a replacement after this
// probe is outside the guard and requires quiescent rollout.
func validateStrictStringNumericCompatibilityDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil || p.Node.Addr == "" {
		return moerr.NewNotSupportedNoCtx(
			"string numeric compatibility requires a known v103 remote destination",
		)
	}
	if proc.GetSessionInfo().LegacyNumericCompatibilityMode {
		return moerr.NewNotSupportedNoCtx(
			"string numeric compatibility cannot run with a legacy session contract",
		)
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		defines.MORPCVersion103,
	)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"string numeric compatibility requires MORPC protocol version 103 on the remote destination",
		)
	}
	return nil
}

// validateStringNumericResultDestination rechecks the actual serialized
// destination at send time. A worker can be downgraded or replaced after
// compile-time placement, so coordinator-only version checks are insufficient.
func validateStringNumericResultDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"corrected string numeric result contracts require a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		defines.MORPCVersion80,
	)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support corrected string numeric result contracts (MORPC version 80)",
		)
	}
	return nil
}
