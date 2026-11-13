//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	activeM0CaptureStatementView = captureM0StatementView
	activeM0CloneCreateContext = bindM0CloneCreateContext
	activeM0Enabled = true
	activeM0DisableCountNotNullRewrite = true
	activeM0ValidateRemoteReadBinding = validateM0RemoteReadBinding
	activeM0CaptureRemoteReadGeneration = captureM0RemoteReadGeneration
	activeM0WaitRemoteReadSnapshot = waitM0RemoteReadSnapshot
}

func bindM0CloneCreateContext(c *Compile, clone *plan.CloneTable) (context.Context, error) {
	if clone.SrcObjDef == nil || clone.SrcTableDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: clone source identity unavailable")
	}
	node := &plan.Node{ObjRef: clone.SrcObjDef, TableDef: clone.SrcTableDef, ScanSnapshot: clone.ScanSnapshot}
	rel, _, ctx, err := c.handleDbRelContextWithBoundTxn(node, false, c.proc.Ctx, nil)
	if err != nil {
		return nil, err
	}
	if rel.GetTableID(ctx) != clone.SrcTableDef.TblId || rel.GetDBID(ctx) != clone.SrcTableDef.DbId {
		return nil, moerr.NewInternalErrorNoCtx("M0: clone source relation binding mismatch")
	}
	if rel.GetExtraInfo() == nil {
		return m0read.WithCloneEvolution(c.proc.Ctx, nil), nil
	}
	contract := rel.GetExtraInfo().M0Evolution
	if contract != nil {
		account, err := defines.GetAccountId(ctx)
		if err != nil {
			return nil, err
		}
		owner := m0read.EvolutionBinding{Account: account, Database: rel.GetDBID(ctx), Relation: rel.GetTableID(ctx)}
		if err := m0read.ValidateEvolution(contract, owner); err != nil {
			return nil, err
		}
	}
	return m0read.WithCloneEvolution(c.proc.Ctx, contract), nil
}

// Capture after transaction statement preparation and before constructing scan
// scopes. The current logical plan determines need; ordinary tables keep their
// existing workspace path, including tagged ordinary partition queries.
func captureM0StatementView(c *Compile, p *plan.Plan) (m0read.StatementReadView, error) {
	if p == nil || p.GetQuery() == nil {
		return nil, nil
	}
	needed := false
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType != plan.Node_TABLE_SCAN && n.NodeType != plan.Node_TABLE_CLONE {
			continue
		}
		contract, err := m0read.PlanEvolution(n.TableDef)
		if err != nil {
			return nil, err
		}
		needed = needed || contract != nil
	}
	if !needed {
		return nil, nil
	}
	op := c.proc.GetTxnOperator()
	if op == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: statement transaction unavailable")
	}
	owner, ok := op.GetWorkspace().(interface {
		CaptureM0StatementReadView(context.Context, int, *process.Process) (m0read.StatementReadView, error)
	})
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("M0: statement workspace owner unavailable")
	}
	return owner.CaptureM0StatementReadView(c.proc.Ctx, c.TxnOffset, c.proc)
}
