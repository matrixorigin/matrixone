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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/gpumode"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
)

// Required PRE can only use workers that can receive the complete local domain
// and honor a CPU centroid route. A GPU route, a non-coordinator ingress or a
// write transaction falls back as a whole query.
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
		return nil
	}
	var err error
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}
