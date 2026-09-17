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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSON MIN/MAX partials are compared by the typed ByteJSON ordering. An old
// receiver would silently compare the same payload bytewise, so this semantic
// change gets its own cumulative protocol gate instead of relying on the
// payload wire format being decodable by both versions.
func hasJSONMinMaxAggregate(node *plan.Node) bool {
	if node == nil {
		return false
	}
	for _, expr := range node.AggList {
		if exprUsesJSONMinMax(expr) {
			return true
		}
	}
	for _, expr := range node.WinSpecList {
		if expr == nil || expr.GetW() == nil {
			continue
		}
		if exprUsesJSONMinMax(expr.GetW().GetWindowFunc()) {
			return true
		}
	}
	return false
}

func exprUsesJSONMinMax(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		return false
	}
	id := int64(uint64(fn.Func.Obj) & function.DistinctMask)
	if id != aggexec.AggIdOfMin && id != aggexec.AggIdOfMax {
		return false
	}
	return fn.Args[0] != nil && types.T(fn.Args[0].Typ.Id) == types.T_json
}

func queryUsesJSONMinMax(qry *plan.Query) bool {
	if qry == nil {
		return false
	}
	for _, node := range qry.Nodes {
		if hasJSONMinMaxAggregate(node) {
			return true
		}
	}
	return false
}

func (c *Compile) constrainJSONMinMaxWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN || !queryUsesJSONMinMax(qry) {
		return nil
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion85)
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

func supportsRemoteJSONMinMax(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	protocolVersion, valid := version.(int64)
	return ok && valid && protocolVersion >= defines.MORPCVersion85
}

func (c *Compile) supportsRemoteJSONMinMax() bool {
	return c != nil && c.proc != nil && supportsRemoteJSONMinMax(c.proc.GetService())
}

// pipelineUsesJSONMinMax inspects the aggregate representation that is
// actually serialized. Aggregate functions no longer appear in the ordinary
// expression tree after lowering to pipeline.Aggregate.Op/Expr.
func pipelineUsesJSONMinMax(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, instruction := range p.InstructionList {
		if instruction == nil || instruction.Agg == nil {
			continue
		}
		for _, agg := range instruction.Agg.Aggs {
			if agg == nil || (agg.Op != aggexec.AggIdOfMin && agg.Op != aggexec.AggIdOfMax) || len(agg.Expr) == 0 {
				continue
			}
			if agg.Expr[0] != nil && types.T(agg.Expr[0].Typ.Id) == types.T_json {
				return true
			}
		}
	}
	for _, child := range p.Children {
		if pipelineUsesJSONMinMax(child) {
			return true
		}
	}
	return false
}

// validateJSONMinMaxDestination rechecks the selected remote CN after
// compilation. Placement can become stale during a rolling upgrade; an old
// receiver must reject this semantic payload instead of silently using its
// legacy bytewise comparator.
func validateJSONMinMaxDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if !pipelineUsesJSONMinMax(p) {
		return nil
	}
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"JSON MIN/MAX remote execution requires a versioned remote destination",
		)
	}
	supported, err := remoteWorkersSupportProtocol(proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion85)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"JSON MIN/MAX remote execution requires MORPC protocol version 85",
		)
	}
	return nil
}
