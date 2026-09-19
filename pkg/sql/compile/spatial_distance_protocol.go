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

// constrainSpatialDistanceWorkers keeps changed spatial-distance expressions
// on one CN while a rolling cluster still contains workers below MORPC v88.
// The capability probe is performed once per compile and has no per-row cost.
func (c *Compile) constrainSpatialDistanceWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil || !features.SpatialDistanceSemantics {
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

// validateSpatialDistanceDestination rechecks the actual serialized
// destination at send time. A worker can be downgraded or replaced after
// compile-time placement, so coordinator-only gating is insufficient.
func validateSpatialDistanceDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"geodetic spatial-distance semantics require a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion88)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support geodetic spatial-distance semantics (MORPC version 88)",
		)
	}
	return nil
}
