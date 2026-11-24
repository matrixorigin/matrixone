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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// These are pipeline.Instruction.Op wire aliases, not executable VM operators.
// Reserve them outside the VM opcode range. A receiver that doesn't understand
// an alias rejects the fragment before executing any operator. An upgraded
// receiver must not unilaterally change ownership for an unversioned fragment.
const (
	wireIvfSearchStableOwnership      int32 = 1024
	wireIvfEntriesScanStableOwnership int32 = 1025
)

type stableIvfOwnershipKey struct{}

type ivfOwnershipMode uint8

const (
	ivfOwnershipAbsent ivfOwnershipMode = iota
	ivfOwnershipLegacy
	ivfOwnershipStable
)

func stableIvfOwnership(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	remote, _ := ctx.Value(defines.RemoteRunContext{}).(bool)
	if !remote {
		return true
	}
	stable, _ := ctx.Value(stableIvfOwnershipKey{}).(bool)
	return stable
}

func ivfOwnershipWireAlias(p *pipeline.Pipeline, in *pipeline.Instruction, op vm.OpType) int32 {
	switch op {
	case vm.TableFunction:
		tf := in.GetTableFunction()
		if tf.GetName() == ivfflatplan.IVFFLATSearchFuncName && tf.GetIndexReaderParam().GetOrigFuncName() != "" {
			return wireIvfSearchStableOwnership
		}
	case vm.TableScan:
		node := p.GetDataSource().GetNode()
		if in.GetTableScan() != nil && p.GetNode().GetCnCnt() > 1 &&
			node.GetNodeType() == plan.Node_TABLE_SCAN && plan2.IsIvfSearchEntriesInternalScan(node) {
			return wireIvfEntriesScanStableOwnership
		}
	}
	return 0
}

func decodeIvfOwnershipInstruction(root *scopeContext, p *pipeline.Pipeline, in *pipeline.Instruction) error {
	mode := ivfOwnershipStable
	op := vm.OpType(in.GetOp())
	switch in.GetOp() {
	case wireIvfSearchStableOwnership:
		op = vm.TableFunction
	case wireIvfEntriesScanStableOwnership:
		op = vm.TableScan
	default:
		mode = ivfOwnershipLegacy
	}
	alias := ivfOwnershipWireAlias(p, in, op)
	if mode == ivfOwnershipStable && alias != in.GetOp() {
		return moerr.NewInvalidInputNoCtx("invalid IVF ownership instruction payload")
	}
	if alias == 0 {
		return nil
	}
	if root.ivfOwnership != ivfOwnershipAbsent && root.ivfOwnership != mode {
		return moerr.NewInvalidInputNoCtx("mixed IVF ownership instructions in remote fragment")
	}
	root.ivfOwnership, in.Op = mode, int32(op)
	return nil
}
