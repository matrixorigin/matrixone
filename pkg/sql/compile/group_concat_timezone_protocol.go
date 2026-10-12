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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func groupConcatTimeZoneRequirement(proc *process.Process, owner any) (named, local bool, err error) {
	required, err := plan.RequiresGroupConcatTimeZone(owner)
	if p, ok := owner.(*pipeline.Pipeline); ok && !required && err == nil {
		required = pipelineRequiresGroupConcatTimeZone(p)
	}
	if err != nil || !required {
		return false, false, err
	}
	if proc == nil || proc.Base == nil {
		return false, true, nil
	}
	location := proc.GetSessionInfo().TimeZone
	if location == nil {
		return false, true, nil
	}
	name := process.TimeZoneLocationName(location)
	return name != "", name == "" && !process.IsFixedTimeZone(location), nil
}

// Aggregate functions are lowered to Aggregate.Op and argument-only Expr lists
// on the wire. Inspect these in addition to the remaining plan expressions.
func pipelineRequiresGroupConcatTimeZone(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, in := range p.InstructionList {
		if in == nil || in.Agg == nil {
			continue
		}
		for _, agg := range in.Agg.Aggs {
			if agg == nil || agg.Op != aggexec.AggIdOfGroupConcat {
				continue
			}
			for _, arg := range agg.Expr {
				if arg != nil && arg.Typ.Id == int32(types.T_timestamp) {
					return true
				}
			}
		}
	}
	for _, child := range p.Children {
		if pipelineRequiresGroupConcatTimeZone(child) {
			return true
		}
	}
	return false
}
func (c *Compile) constrainGroupConcatTimeZoneWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	named, local, err := groupConcatTimeZoneRequirement(c.proc, qry)
	if err != nil {
		return err
	}
	if named && !local {
		var supported bool
		supported, err = remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion67)
		if err != nil {
			return err
		}
		local = !supported
	}
	if !local {
		return nil
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}
func validateGroupConcatTimeZoneDestination(proc *process.Process, p *pipeline.Pipeline) error {
	named, local, err := groupConcatTimeZoneRequirement(proc, p)
	if err != nil {
		return err
	}
	if !named && !local {
		return nil
	}
	if !local && p != nil && p.Node != nil {
		supported, err := remoteWorkersSupportProtocol(proc, engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion67)
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx("remote GROUP_CONCAT requires a portable time zone and MORPC version 67")
}
