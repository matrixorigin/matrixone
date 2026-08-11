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
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	disttaesidecar "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/sidecar"
)

// SiriusReadPlan is an admitted Substrait plan plus the leases its eventual
// execution owner must release on every terminal path.
type SiriusReadPlan struct {
	Plan        []byte
	ReadRefs    [][]byte
	OutputTypes []planpb.Type
	Headings    []string
}

func (p *SiriusReadPlan) Release(ctx context.Context, leases *substrait.LeaseManager) error {
	if p == nil || leases == nil {
		return nil
	}
	var result error
	for _, ref := range p.ReadRefs {
		if err := leases.Release(ctx, ref); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// CompileSiriusRead runs at the logical-plan cutpoint, before compileScope.
// Export validates the whole tree before this function opens any relation.
// The opt-in compile path passes the selected sidecar client's TLS SPKI hash
// and transfers lease ownership to execution. The execution owner must
// cancel and join the sidecar request before calling SiriusReadPlan.Release.
func (c *Compile) CompileSiriusRead(ctx context.Context, queryPlan *planpb.Plan, accountID uint64, queryID, authorizedClientSPKIHash []byte, dataDir string, ttl time.Duration, leases *substrait.LeaseManager) (*SiriusReadPlan, error) {
	if c == nil || queryPlan == nil {
		return nil, moerr.NewInternalError(ctx, "substrait: compile has no query plan")
	}
	if queryPlan.GetQuery() == nil {
		return nil, substrait.NotEligible(substrait.EligibilityPlanShape, "statement is not a SELECT query")
	}
	candidate, err := substrait.Export(queryPlan.GetQuery())
	if err != nil {
		return nil, err
	}
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil || txnOp.GetWorkspace() == nil {
		return nil, moerr.NewInternalError(ctx, "substrait: compile has no transaction workspace")
	}
	ws := txnOp.GetWorkspace()
	readOnly := ws.Readonly()
	priorWrites := ws.WriteOffset() != 0 || ws.GetSnapshotWriteOffset() != 0
	if !readOnly || priorWrites {
		return nil, substrait.NotEligible(substrait.EligibilityTransaction, "transaction is not an admissible read-only snapshot")
	}
	if leases == nil || !leases.Ready() || !leases.Protected() || accountID == 0 || len(queryID) == 0 || ttl <= 0 || ttl > substrait.MaxLeaseTTL {
		return nil, moerr.NewInternalError(ctx, "substrait: invalid Sirius admission configuration")
	}
	relations := make(map[uint64]engine.Relation, len(candidate.Reads()))
	for _, read := range candidate.Reads() {
		node := queryPlan.GetQuery().Nodes[read.NodeID]
		rel, _, _, openErr := c.handleDbRelContext(node, false)
		if openErr != nil {
			return nil, moerr.NewInternalErrorf(ctx, "substrait: open table %d: %v", read.TableID, openErr)
		}
		relations[read.TableID] = rel
	}
	provider := &disttaesidecar.SnapshotProvider{Relations: relations, MPool: c.proc.Mp(), DataDir: dataDir, TxnOffset: ws.GetSnapshotWriteOffset()}
	snapshot := types.TimestampToTS(txnOp.SnapshotTS())
	snapshotBytes, err := snapshot.Marshal()
	if err != nil {
		return nil, err
	}
	wires, err := substrait.Admit(ctx, substrait.AdmissionRequest{Candidate: candidate, Provider: provider, Leases: leases, AccountID: accountID, QueryID: queryID, SnapshotTS: snapshotBytes, AuthorizedClientSPKIHash: authorizedClientSPKIHash, TTL: ttl, ReadOnly: readOnly, PriorWrites: priorWrites})
	if err != nil {
		return nil, err
	}
	result := &SiriusReadPlan{
		ReadRefs: make([][]byte, 0, len(wires)), OutputTypes: candidate.OutputTypes(),
		Headings: append([]string(nil), queryPlan.GetQuery().Headings...),
	}
	for _, candidateRead := range candidate.Reads() {
		read, decodeErr := substrait.UnmarshalTaeRead(wires[candidateRead.NodeID], 0)
		if decodeErr != nil {
			var releaseErr error
			for _, wire := range wires {
				if admitted, ok := substrait.UnmarshalTaeRead(wire, 0); ok == nil {
					releaseErr = errors.Join(releaseErr, leases.Release(ctx, admitted.ReadRef))
				}
			}
			return nil, errors.Join(decodeErr, releaseErr)
		}
		result.ReadRefs = append(result.ReadRefs, read.ReadRef)
	}
	result.Plan, err = candidate.Build(wires)
	if err != nil {
		releaseErr := result.Release(ctx, leases)
		// Export already proved eligibility before any storage work. A Build
		// failure after admission is operational and must never trigger fallback
		// after durable leases have been published.
		buildErr := moerr.NewInternalErrorf(ctx, "substrait: build admitted plan: %v", err)
		return nil, errors.Join(buildErr, releaseErr)
	}
	return result, nil
}
