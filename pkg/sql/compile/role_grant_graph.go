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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func roleGrantGraphFrontier(ctx context.Context, eng engine.Engine, service string) (timestamp.Timestamp, error) {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "missing service runtime for role grant graph")
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if ok && valid && version >= defines.MORPCVersion11 {
		barrier, available := getLogtailReadBarrier(eng)
		if !available {
			return timestamp.Timestamp{}, moerr.NewNotSupported(ctx,
				"role membership changes are not supported without a logtail read barrier")
		}
		return barrier.AcquireLogtailReadBarrier(ctx)
	}
	clock := rt.Clock()
	if clock == nil || clock.MaxOffset() < 0 {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "invalid transaction clock for role grant graph")
	}
	_, upperBound := clock.Now()
	if upperBound.PhysicalTime < 0 || upperBound.PhysicalTime == math.MaxInt64 {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "role grant graph snapshot timestamp overflow")
	}
	// During a rolling upgrade, older TN services cannot provide the read
	// barrier. The HLC uncertainty upper bound is later than every commit
	// completed before this lock. UpdateSnapshot waits for local logtail apply.
	return timestamp.Timestamp{PhysicalTime: upperBound.PhysicalTime + 1}, nil
}

func getLogtailReadBarrier(eng engine.Engine) (engine.LogtailReadBarrier, bool) {
	for eng != nil {
		if entire, ok := eng.(*engine.EntireEngine); ok {
			if entire == nil || entire.Engine == nil {
				return nil, false
			}
			eng = entire.Engine
			continue
		}
		barrier, ok := eng.(engine.LogtailReadBarrier)
		return barrier, ok
	}
	return nil, false
}

// LockRoleGrantGraph serializes role membership changes for this tenant.
// The caller must retain the same pessimistic RC transaction through validation
// and commit or rollback. A table lock protects the graph-wide acyclicity
// predicate; the read barrier makes earlier committed edges visible before
// the caller reads the graph.
func LockRoleGrantGraph(ctx context.Context, eng engine.Engine, proc *process.Process) error {
	if proc == nil || eng == nil {
		return moerr.NewInternalError(ctx, "role grant graph requires an engine process")
	}
	txnOp := proc.GetTxnOperator()
	if txnOp == nil || !txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		return moerr.NewNotSupported(ctx,
			"role membership changes are not supported outside pessimistic read-committed transactions")
	}
	db, err := eng.Database(ctx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	rel, err := db.Relation(ctx, catalog.MO_ROLE_GRANT, nil)
	if err != nil {
		return err
	}
	if err := lockTableForSnapshotRefresh(ctx, eng, proc, rel, "", false); err != nil {
		return err
	}
	return advanceCatalogLifecycleSnapshot(ctx, catalogLifecycleSnapshotHooks{
		acquireReadBarrier: func(ctx context.Context) (timestamp.Timestamp, error) {
			return roleGrantGraphFrontier(ctx, eng, proc.GetService())
		},
		currentSnapshot: txnOp.SnapshotTS,
		updateSnapshot:  txnOp.UpdateSnapshot,
	}, "role grant graph")
}
