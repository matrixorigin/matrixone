// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
)

func (c *Compile) CompileSiriusStreamRead(
	ctx context.Context,
	queryPlan *planpb.Plan,
	accountID uint64,
	queryID []byte,
	ttl time.Duration,
) (*SiriusReadPlan, error) {
	if c == nil || c.proc == nil || queryPlan == nil || queryPlan.GetQuery() == nil {
		return nil, moerr.NewInternalError(ctx, "substrait: stream compile has no SELECT plan")
	}
	candidate, err := substrait.Export(queryPlan.GetQuery())
	if err != nil {
		return nil, err
	}
	reads, err := candidate.StreamReads()
	if err != nil {
		return nil, err
	}
	if len(reads) == 0 || len(reads) > 16 || len(queryID) != 16 || ttl <= 0 {
		return nil, substrait.NotEligible(substrait.EligibilityPlanShape, "streamed read count or identity is unsupported")
	}
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil || txnOp.GetWorkspace() == nil || !txnOp.GetWorkspace().Readonly() ||
		txnOp.GetWorkspace().WriteOffset() != 0 || txnOp.GetWorkspace().GetSnapshotWriteOffset() != 0 {
		return nil, substrait.NotEligible(substrait.EligibilityTransaction, "stream mode requires a read-only snapshot without prior writes")
	}
	snapshot := types.TimestampToTS(txnOp.SnapshotTS())
	snapshotBytes, err := snapshot.Marshal()
	if err != nil {
		return nil, err
	}
	expires := time.Now().Add(ttl)
	bindings := make(map[int32]substrait.ReadBinding, len(reads))
	inputs := make([]SiriusStreamInput, 0, len(reads))
	for _, read := range reads {
		if read.Occurrences != 1 {
			return nil, substrait.NotEligible(substrait.EligibilityPlanShape, "stream mode does not replay shared scan nodes")
		}
		ref := make([]byte, 32)
		if _, err = rand.Read(ref); err != nil {
			return nil, err
		}
		digest := sha256.Sum256(read.StreamSchema)
		wire, marshalErr := substrait.MarshalStreamRead(&substrait.StreamRead{
			ProtocolVersion: substrait.StreamReadProtocolVersion,
			StreamRef:       ref, QueryID: append([]byte(nil), queryID...), AccountID: accountID,
			SnapshotTS: snapshotBytes, SchemaDigest: digest[:], CapabilityHash: substrait.CapabilityHash[:],
			ExpiresAtUnixMS: uint64(expires.UnixMilli()),
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		bindings[read.NodeID] = substrait.ReadBinding{
			TypeURL: substrait.StreamReadTypeURL, Value: wire, Schema: read.StreamSchema,
		}
		inputs = append(inputs, SiriusStreamInput{
			NodeID: read.NodeID, StreamRef: append([]byte(nil), ref...),
		})
	}
	planBytes, err := candidate.BuildWithBindings(bindings)
	if err != nil {
		return nil, err
	}
	return &SiriusReadPlan{
		Plan: planBytes, OutputTypes: candidate.OutputTypes(),
		Headings:       append([]string(nil), queryPlan.GetQuery().Headings...),
		LeaseExpiresAt: expires, StreamInputs: inputs,
	}, nil
}

func (c *Compile) tryCompileSiriusStreamRead(
	ctx context.Context,
	queryPlan *planpb.Plan,
	runtime *SiriusRuntime,
) (bool, error) {
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}
	accountID := uint64(account)
	statementID := c.proc.GetStmtProfile().GetStmtId()
	queryID := append([]byte(nil), statementID[:]...)
	readPlan, err := c.CompileSiriusStreamRead(ctx, queryPlan, accountID, queryID, runtime.LeaseTTL)
	if err != nil {
		return false, err
	}
	c.initSiriusStreamCompile(queryPlan)
	execution, err := runtime.Flight.Prepare(
		ctx, accountID, queryID, readPlan.Plan, readPlan.OutputTypes, readPlan.Headings,
		readPlan.LeaseExpiresAt.Add(-runtime.CleanupTimeout), func(context.Context) error { return nil },
	)
	if err != nil {
		return false, err
	}
	inputs, scopes, err := c.compileSiriusStreamScopes(queryPlan.GetQuery(), readPlan.StreamInputs, execution)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtime.CleanupTimeout)
		defer cancel()
		return false, errors.Join(err, execution.Cleanup(cleanupCtx))
	}
	c.scopes = scopes
	c.siriusRead = newSiriusStreamOwner(execution, runtime, inputs)
	return true, nil
}

func (c *Compile) initSiriusStreamCompile(queryPlan *planpb.Plan) {
	execType := plan2.GetExecType(queryPlan.GetQuery(), c.getHaveDDL(), c.isPrepare)
	if execType == plan2.ExecTypeAP_MULTICN {
		execType = plan2.ExecTypeAP_ONECN
	}
	c.execType = execType
	ncpu := int32(c.ncpu)
	if ncpu < 1 {
		ncpu = 1
	}
	plan2.CalcQueryDOP(queryPlan, ncpu, 1, execType)
	c.initAnalyzeModule(queryPlan.GetQuery())
}

func (c *Compile) compileSiriusLocalTableScan(node *planpb.Node) ([]*Scope, error) {
	if _, _, _, err := c.handleDbRelContext(node, false); err != nil {
		return nil, err
	}
	local := getEngineNode(c)
	local.Addr = c.addr
	if node.Stats != nil && node.Stats.Dop > 0 {
		local.Mcpu = min(local.Mcpu, int(node.Stats.Dop))
	}
	local.Mcpu = normalizeMcpu(local.Mcpu)
	local.CNCNT = 1
	local.CNIDX = 0
	scope, err := c.compileTableScanWithNode(node, local, c.anal.isFirst)
	if err != nil {
		return nil, err
	}
	c.anal.isFirst = false
	return []*Scope{scope}, nil
}

func (c *Compile) compileSiriusStreamScopes(
	query *planpb.Query,
	streamInputs []SiriusStreamInput,
	execution *sidecarflight.Execution,
) ([]*sidecarflight.NativeInput, []*Scope, error) {
	inputs := make([]*sidecarflight.NativeInput, 0, len(streamInputs))
	roots := make([]*Scope, 0, len(streamInputs))
	succeeded := false
	defer func() {
		if !succeeded {
			ReleaseScopes(roots)
		}
	}()
	for _, spec := range streamInputs {
		if spec.NodeID < 0 || int(spec.NodeID) >= len(query.Nodes) || query.Nodes[spec.NodeID] == nil {
			return nil, nil, moerr.NewInternalErrorNoCtx("substrait: streamed scan node is missing")
		}
		node := plan2.DeepCopyNode(query.Nodes[spec.NodeID])
		c.appendMetaTables(node.ObjRef)
		node.RuntimeFilterProbeList = nil
		node.RuntimeFilterBuildList = nil
		node.RecvMsgList = nil
		// Scan-level aggregation is a native storage optimization. The streamed
		// relation must contain ordinary post-filter/project rows because the
		// semantic aggregate remains in the exported Substrait plan.
		node.AggList = nil
		if len(node.ProjectList) == 0 {
			for position, column := range node.TableDef.Cols {
				if column == nil || column.Hidden {
					continue
				}
				node.ProjectList = append(node.ProjectList, &planpb.Expr{
					Typ:  column.Typ,
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(position)}},
				})
			}
		}
		c.setAnalyzeCurrent(nil, int(spec.NodeID))
		scans, err := c.compileSiriusLocalTableScan(node)
		if err != nil {
			return nil, nil, err
		}
		scans = c.compileTableScanFiltersAndProjection(node, scans)
		if node.Offset != nil {
			scans = c.compileOffset(node, scans)
		}
		if node.Limit != nil {
			scans = c.compileLimit(node, scans)
		}
		root := c.newMergeScope(scans)
		roots = append(roots, root)
		nativeInput, err := execution.NewNativeInput(spec.StreamRef)
		if err != nil {
			return nil, nil, err
		}
		root.setRootOperator(output.NewArgument().WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
			if bat == nil {
				return nativeInput.Finish(c.proc.Ctx)
			}
			return nativeInput.Send(c.proc.Ctx, bat, c.proc.Mp())
		}).WithShouldStop(nativeInput.NotNeeded))
		inputs = append(inputs, nativeInput)
	}
	succeeded = true
	return inputs, roots, nil
}
