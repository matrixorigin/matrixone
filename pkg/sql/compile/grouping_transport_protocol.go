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

func queryNeedsGroupingTransport(q *plan.Query) bool {
	if q == nil {
		return false
	}
	for _, n := range q.Nodes {
		if n == nil {
			continue
		}
		if _, ok := plan2.DecodeGroupingSetExpandOption(n.ExtraOptions); ok {
			return true
		}
		for _, active := range n.GroupingFlag {
			if !active {
				return true
			}
		}
	}
	return false
}

// Preserve the query requirement on forwarding-only scopes too. The existing
// pipeline Qry field carries it once per remote scope, including redispatch.
func attachGroupingTransportPlan(scopes []*Scope, p *plan.Plan) {
	for _, s := range scopes {
		if s == nil {
			continue
		}
		s.Plan = p
		attachGroupingTransportPlan(s.PreScopes, p)
	}
}

func requireGroupingTransportWorkers(proc *process.Process, workers engine.Nodes) error {
	supported, err := remoteWorkersSupportProtocol(proc, workers, defines.MORPCVersion87)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx("remote grouping provenance requires MORPC protocol version 87 on every participating CN")
	}
	return nil
}

func (c *Compile) validateGroupingTransportPlacement(q *plan.Query) error {
	if !queryNeedsGroupingTransport(q) {
		return nil
	}
	for _, worker := range c.cnList {
		// Scheduled local routes have already been materialized to c.addr.
		if worker.Addr != c.addr || c.addr == "" {
			return requireGroupingTransportWorkers(c.proc, c.cnList)
		}
	}
	return nil
}

// Recheck actual endpoints, not just placement candidates. A forwarding scope
// may have no Group operator yet still receive and dispatch grouping vectors.
// Rolling back binaries must drain in-flight queries; this is not a negotiated
// connection protocol and cannot protect a connection replaced after the probe.
func validateGroupingTransportDestinations(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || !queryNeedsGroupingTransport(p.GetQry().GetQuery()) {
		return nil
	}
	workers := make(engine.Nodes, 0)
	seen := make(map[string]bool)
	add := func(id, addr string) {
		if !seen[addr] {
			seen[addr] = true
			workers = append(workers, engine.Node{Id: id, Addr: addr})
		}
	}
	var visit func(*pipeline.Pipeline)
	visit = func(p *pipeline.Pipeline) {
		if p == nil {
			return
		}
		if p.Node != nil && p.Node.Addr != "" {
			add(p.Node.Id, p.Node.Addr)
		}
		for _, reg := range p.UuidsToRegIdx {
			if reg != nil {
				add("", reg.FromAddr)
			}
		}
		for _, in := range p.InstructionList {
			for _, dest := range in.GetDispatch().GetRemoteConnector() {
				if dest != nil {
					add("", dest.NodeAddr)
				}
			}
		}
		for _, child := range p.Children {
			visit(child)
		}
	}
	if p.Node == nil || p.Node.Addr == "" {
		return moerr.NewNotSupportedNoCtx("remote grouping provenance requires a known destination")
	}
	visit(p)
	return requireGroupingTransportWorkers(proc, workers)
}
