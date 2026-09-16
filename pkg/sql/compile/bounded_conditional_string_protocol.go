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

// constrainBoundedConditionalStringWorkers keeps the new bounded binary
// COALESCE overloads on one CN while a rolling cluster contains pre-v82
// workers. Those workers do not have overload identities 30 and 31.
func (c *Compile) constrainBoundedConditionalStringWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !features.BoundedConditionalStringDomains {
		return err
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion82)
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

// validateBoundedConditionalStringDestination closes the race between worker
// placement and serialization if a selected worker is replaced or downgraded.
func validateBoundedConditionalStringDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"bounded conditional string domains require a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		defines.MORPCVersion82,
	)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support bounded conditional string domains (MORPC version 82)",
		)
	}
	return nil
}
