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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mongoscan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

// The ordinary source compiler owns authorization, catalog resolution and file
// metadata. It runs once on a private node. Its unprepared operator is retained
// only as metadata; no nested Scope is scheduled by a dependent invocation.
func (c *Compile) compileDependentExternal(node *plan.Node) (dependent.SourceFactory, error) {
	if node.ObjRef != nil {
		c.appendMetaTables(node.ObjRef)
	}
	var deferred []*plan.Expr
	static := make([]*plan.Expr, 0, len(node.FilterList))
	for _, expr := range node.FilterList {
		dynamic := false
		_ = plan.VisitExprTree(expr, func(e *plan.Expr) error {
			dynamic = dynamic || e.GetDependent() != nil || e.GetSlot() != nil
			return nil
		})
		if dynamic {
			deferred = append(deferred, expr)
		} else {
			static = append(static, expr)
		}
	}
	node.FilterList = static
	loadTag := c.proc.Base.LoadTag
	scopes, err := c.compileExternScanSource(node, node.NodeId, false, true)
	c.proc.Base.LoadTag = loadTag
	node.FilterList = append(node.FilterList, deferred...)
	// Keep even partial metadata owners on the Compile error-cleanup path.
	c.dependentSourceScopes = append(c.dependentSourceScopes, scopes...)
	if err != nil {
		return nil, err
	}
	if len(scopes) != 1 || len(scopes[0].PreScopes) != 0 || len(scopes[0].RootOp.GetOperatorBase().Children) != 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "dependent external metadata contains a worker pipeline")
	}
	metadata := scopes[0].RootOp
	switch metadata.(type) {
	case *external.External, *mongoscan.MongoScan, *value_scan.ValueScan:
	default:
		return nil, moerr.NewInternalErrorf(c.proc.Ctx, "unexpected dependent source metadata %T", metadata)
	}
	metadataSize := max(unsafe.Sizeof(external.External{})+unsafe.Sizeof(external.ExternalParam{})+unsafe.Sizeof(external.ExFileparam{})+unsafe.Sizeof(external.FilterParam{}), unsafe.Sizeof(mongoscan.MongoScan{}), unsafe.Sizeof(value_scan.ValueScan{}))
	return dependentSourceFactory(uint64(metadataSize), func(proc *process.Process, allocation *vector.AllocationAccountSelection) (vm.Operator, error) {
		var op vm.Operator
		switch meta := metadata.(type) {
		case *external.External:
			param := &external.ExternalParam{ExParamConst: meta.Es.ExParamConst}
			if meta.Es.Extern != nil {
				externalParam := *meta.Es.Extern
				externalParam.Ctx = proc.Ctx
				param.Extern = &externalParam
			}
			param.Ctx = proc.Ctx
			param.Fileparam = &external.ExFileparam{}
			param.Filter = &external.FilterParam{}
			if meta.Es.Filter != nil {
				param.Filter.FilterExpr = meta.Es.Filter.FilterExpr
			}
			if param.IcebergObjectIORef != "" && !icebergio.RetainObjectIORef(param.IcebergObjectIORef) {
				return nil, moerr.NewInvalidInput(proc.Ctx, "dependent Iceberg source metadata expired")
			}
			source := external.NewArgument().WithEs(param)
			source.OutputAllocation = allocation
			op = source
		case *mongoscan.MongoScan:
			source := mongoscan.NewArgument().WithScan(meta.Scan)
			source.OutputAllocation = allocation
			op = source
		case *value_scan.ValueScan:
			// No matched files is an empty relation, not the SELECT-without-FROM row.
			op = value_scan.NewArgument()
		}
		op.GetOperatorBase().SetAnalyzeControl(int(node.NodeId), true)
		return op, nil
	}), nil
}

func (c *Compile) clearDependentSourceMetadata() {
	for _, s := range c.dependentSourceScopes {
		s.FreeOperator(c)
		s.release()
	}
	c.dependentSourceScopes = nil
}
