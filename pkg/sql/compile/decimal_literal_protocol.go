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

// constrainDecimalLiteralWorkers keeps exact DECIMAL256 literal plans local
// while a rolling cluster still contains workers below MORPC v88. The marker
// is execution-inert on this CN, but an older worker may rebind the persisted
// spelling with a different decimal carrier.
func (c *Compile) constrainDecimalLiteralWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !features.DecimalLiteralSemantics {
		return err
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion88)
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

// validateDecimalLiteralDestination closes the placement/send race for a
// marked literal. The coordinator may have selected a worker before it was
// downgraded or replaced, so the actual destination must be checked again.
func validateDecimalLiteralDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"exact DECIMAL256 literal semantics require a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		defines.MORPCVersion88,
	)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support exact DECIMAL256 literal semantics (MORPC protocol version 88)",
		)
	}
	return nil
}
