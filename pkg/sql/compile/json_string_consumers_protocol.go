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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// constrainJSONStringConsumerWorkers keeps plans containing overload 1 of
// CONCAT, CONCAT_WS, or ELT on one CN until every selected worker reports the
// protocol that introduced those identities. The probe is compile-scoped and
// is repeated by the sender for the actual destination.
func (c *Compile) constrainJSONStringConsumerWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !features.JSONStringConsumerOverload {
		return err
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion86)
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

// validateJSONStringConsumerProtocol is the local plan and pipeline admission
// fence. It deliberately scans only when the local process is older or its
// version is unknown, matching the other persistent-plan compatibility gates.
func validateJSONStringConsumerProtocol(proc *process.Process, owner any) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := value.(int64)
			if ok && valid && version >= defines.MORPCVersion86 {
				return nil
			}
		}
	}
	required, err := plan.RequiresMORPCVersion86JSONStringConsumerOverload(owner)
	if err != nil {
		return err
	}
	if !required {
		return nil
	}
	return moerr.NewNotSupportedNoCtxf(
		"JSON string consumer overloads require all CNs to support MORPC protocol version %d",
		defines.MORPCVersion86,
	)
}

// validateJSONStringConsumerDestination rechecks the selected pipeline node
// immediately before serialization. Placement can become stale after compile,
// so coordinator admission alone cannot protect an old worker.
func validateJSONStringConsumerDestination(proc *process.Process, p *pipeline.Pipeline) error {
	features, err := plan.RequiredRemoteExpressionFeatures(p)
	if err != nil {
		return err
	}
	if !features.JSONStringConsumerOverload {
		return nil
	}
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"JSON string consumer overloads require a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		defines.MORPCVersion86,
	)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support JSON string consumer overloads (MORPC protocol version 86)",
		)
	}
	return nil
}
