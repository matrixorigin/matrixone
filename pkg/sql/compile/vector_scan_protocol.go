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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/gpumode"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A fragment's bound handshake must cover every nested index search scan. Any
// IndexSearchScan, as a data source or in APPLY, needs MORPCVersion107; it also
// covers required distributed PRE (103) and coordinator-independent partition
// ownership (96).
func minimumRemoteVectorProtocol(p *pipeline.Pipeline) int64 {
	if p == nil {
		return 0
	}
	var required int64
	if p.DataSource != nil && p.DataSource.Node != nil &&
		p.DataSource.Node.NodeType == plan.Node_INDEX_SEARCH_SCAN {
		required = defines.MORPCVersion107
	}
	for _, in := range p.InstructionList {
		if in.GetApply().GetIndexSearchScan() != nil {
			required = defines.MORPCVersion107
		}
	}
	for _, child := range p.Children {
		required = max(required, minimumRemoteVectorProtocol(child))
	}
	return required
}

func validateRemoteVectorPartitionProtocol(proc *process.Process, p *pipeline.Pipeline) error {
	required := minimumRemoteVectorProtocol(p)
	if required == 0 {
		return nil
	}
	if proc != nil {
		version, known := remoteMORPCProtocolVersion(proc.GetService())
		if known && version >= required {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx(fmt.Sprintf("remote vector partition requires MORPC protocol version %d", required))
}

func validateVectorPartitionDestination(proc *process.Process, p *pipeline.Pipeline) error {
	return validateVectorPartitionDestinationWithResult(proc, p, nil)
}

func validateVectorPartitionDestinationWithResult(proc *process.Process, p *pipeline.Pipeline, requiredVectorProtocol *int64) error {
	required := minimumRemoteVectorProtocol(p)
	if requiredVectorProtocol != nil {
		*requiredVectorProtocol = required
	}
	if required == 0 {
		return nil
	}
	// Use the actual transport destination, including for scans nested below
	// a merge. Re-probe before sending: compilation may predate a rollback.
	if p.Node != nil {
		supported, err := remoteWorkersSupportProtocol(proc,
			engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, required)
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx(fmt.Sprintf("remote destination does not support vector partition (MORPC version %d)", required))
}

// Required PRE can only use workers that can receive the complete local domain
// and honor a CPU centroid route. A GPU route, a non-coordinator ingress or a
// write transaction falls back as a whole query; a worker without
// MORPCVersion107 is an error.
func (c *Compile) constrainRequiredIVFWorkers(qry *plan.Query) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	id, _, _, qualified := plan2.RequiredIVFPlacement(qry)
	if !qualified {
		return nil
	}
	local := getEngineNode(c)
	coordinator := false
	for _, n := range c.cnList {
		coordinator = coordinator || sameExecutionNode(n, local)
	}
	readonly := true
	if txn := c.proc.GetTxnOperator(); txn != nil && txn.GetWorkspace() != nil {
		readonly = txn.GetWorkspace().Readonly()
	}
	gpu := gpumode.EffectiveGpuMode(c.proc.GetResolveVariableFunc())
	device := false
	spec := qry.Nodes[id].IndexSearchScan
	params, parseErr := catalog.IndexParamsStringToMap(spec.Index.IndexAlgoParams)
	if parseErr != nil {
		return parseErr
	}
	part, found := spec.SourceTableDef.Name2ColIndex[spec.Index.Parts[0]]
	if !found || part < 0 || int(part) >= len(spec.SourceTableDef.Cols) {
		return moerr.NewInternalErrorNoCtx("missing IVF vector column")
	}
	// The reader uses float32 centroids for quantized and small element types.
	if types.T(spec.SourceTableDef.Cols[part].Typ.Id) == types.T_array_float64 && params["quantization"] == "" {
		device = brute_force.DispatchesToDevice[float64](gpu)
	} else {
		device = brute_force.DispatchesToDevice[float32](gpu)
	}
	if coordinator && readonly && !device {
		supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion107)
		if err != nil {
			return err
		}
		if !supported {
			return moerr.NewNotSupportedNoCtxf(
				"index search scan requires MORPC protocol version %d on every CN", defines.MORPCVersion107)
		}
		return nil
	}
	var err error
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}
