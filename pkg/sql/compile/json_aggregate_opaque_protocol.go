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

// MORPC v52 describes the scalar MySQL opaque JSON representation. It does
// not establish that a peer has the JSON aggregate consumer, which was added
// here. Keep the aggregate capability on its own protocol version so a
// current-main v80 or reserved v81 parent cannot be admitted as an aggregate
// worker.
const jsonAggregateOpaqueCapabilityVersion = defines.MORPCVersion82

func (c *Compile) constrainJSONAggregateOpaqueWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	required, err := plan.RequiresJSONAggregateOpaqueValues(qry)
	if err != nil || !required {
		return err
	}
	supported, err := remoteWorkersSupportProtocol(
		c.proc, c.cnList, jsonAggregateOpaqueCapabilityVersion)
	if err != nil {
		return err
	}
	if supported {
		return nil
	}
	// A single-CN plan keeps the aggregate state local while a mixed-version
	// cluster rolls out the opaque scalar contract. The executor still checks
	// its local admission value before encoding a non-NULL value.
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

func validateJSONAggregateOpaquePipelineProtocol(
	proc *process.Process,
	p *pipeline.Pipeline,
) error {
	required, err := jsonAggregateOpaqueRequirement(p)
	if err != nil || !required {
		return err
	}
	if proc == nil {
		return jsonAggregateOpaqueProtocolError()
	}
	version, ok := remoteMORPCProtocolVersion(proc.GetService())
	if !ok || version < jsonAggregateOpaqueCapabilityVersion {
		return jsonAggregateOpaqueProtocolError()
	}
	return nil
}

func validateJSONAggregateOpaqueDestination(
	proc *process.Process,
	p *pipeline.Pipeline,
) error {
	required, err := jsonAggregateOpaqueRequirement(p)
	if err != nil || !required {
		return err
	}
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"JSON aggregate opaque values require a target CN protocol capability")
	}
	supported, err := remoteWorkersSupportProtocol(
		proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}},
		jsonAggregateOpaqueCapabilityVersion,
	)
	if err != nil {
		return err
	}
	if !supported {
		return jsonAggregateOpaqueProtocolError()
	}
	return nil
}

func jsonAggregateOpaqueRequirement(owner any) (bool, error) {
	required, err := plan.RequiresJSONAggregateOpaqueValues(owner)
	if err != nil || required {
		return required, err
	}
	if p, ok := owner.(*pipeline.Pipeline); ok {
		required = pipelineRequiresJSONAggregateOpaqueValues(p)
	}
	return required, nil
}

// Aggregate functions are lowered to Aggregate.Op and argument-only Expr
// lists before a pipeline is sent to a worker. Inspect that representation as
// well as the remaining plan expressions so the destination gate cannot be
// bypassed by wire lowering.
func pipelineRequiresJSONAggregateOpaqueValues(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, in := range p.InstructionList {
		if in == nil || in.Agg == nil {
			continue
		}
		for _, agg := range in.Agg.Aggs {
			if agg == nil {
				continue
			}
			valueIndex := -1
			switch agg.Op {
			case aggexec.AggIdOfJsonArrayAgg:
				valueIndex = 0
			case aggexec.AggIdOfJsonObjectAgg:
				valueIndex = 1
			}
			if valueIndex >= 0 && valueIndex < len(agg.Expr) &&
				isJSONAggregateOpaqueType(agg.Expr[valueIndex]) {
				return true
			}
		}
	}
	for _, child := range p.Children {
		if pipelineRequiresJSONAggregateOpaqueValues(child) {
			return true
		}
	}
	return false
}

func validateJSONAggregateOpaqueAggregateProtocol(
	proc *process.Process,
	aggs []aggexec.AggFuncExecExpression,
) error {
	for _, agg := range aggs {
		valueIndex := -1
		switch agg.GetAggID() {
		case aggexec.AggIdOfJsonArrayAgg:
			valueIndex = 0
		case aggexec.AggIdOfJsonObjectAgg:
			valueIndex = 1
		}
		args := agg.GetArgExpressions()
		if valueIndex < 0 || valueIndex >= len(args) ||
			!isJSONAggregateOpaqueType(args[valueIndex]) {
			continue
		}
		if proc == nil {
			return jsonAggregateOpaqueProtocolError()
		}
		version, ok := remoteMORPCProtocolVersion(proc.GetService())
		if !ok || version < jsonAggregateOpaqueCapabilityVersion {
			return jsonAggregateOpaqueProtocolError()
		}
	}
	return nil
}

func isJSONAggregateOpaqueType(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch expr.Typ.Id {
	case int32(types.T_bit),
		int32(types.T_binary),
		int32(types.T_varbinary),
		int32(types.T_blob):
		return true
	default:
		return false
	}
}

func jsonAggregateOpaqueProtocolError() error {
	return moerr.NewNotSupportedNoCtxf(
		"JSON aggregate opaque values require MORPC protocol version %d",
		jsonAggregateOpaqueCapabilityVersion)
}
