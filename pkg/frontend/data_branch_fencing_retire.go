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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// This file is the frontend entry of the pk-move retention-ledger fencing
// retirement critical section (design D168(B)/D170-D177 with D179-D182
// lane mechanics and D183-D185 certification; the production closures live
// in pkg/sql/compile/pk_move_fencing_cs.go). The DROP PITR / DROP SNAPSHOT
// owner-removal reap rides the drop's pessimistic transaction, so the
// retiring transaction, the account, and the CN substrate all resolve from
// the background executor's own session; contexts without a CN substrate
// (unit-test executors, non-CN sessions, non-pessimistic-RC txns,
// pre-upgrade or legacy-shape ledgers) DEGRADE with done=false and the
// caller runs its paired deletes as before.

// runDataBranchFencingRetireForAddress executes one address's retirement
// critical section on the drop's pessimistic transaction and reports the
// locked-view link presence found by the D170(B)(3) re-probe. Degrading is
// silent: the fencing witness is a serialization guard, and every degrade
// class is a context where the guard cannot run (no CN substrate).
func runDataBranchFencingRetireForAddress(
	ctx context.Context, bh BackgroundExec, sid string,
	accountID uint32, tableID uint64,
) (done bool, linkPresence bool, err error) {
	be, ok := bh.(*backExec)
	if !ok || be.backSes == nil {
		return false, false, nil
	}
	txnOp := be.backSes.GetTxnHandler().GetTxn()
	if txnOp == nil {
		return false, false, nil
	}
	pu := getPu(sid)
	var exec executor.SQLExecutor
	if rt := moruntime.ServiceRuntime(sid); rt != nil {
		if val, exist := rt.GetGlobalVariables(moruntime.InternalSQLExecutor); exist {
			exec, _ = val.(executor.SQLExecutor)
		}
	}
	// D186(B.4): the substrate's fresh allocator is OWNED through every
	// outstanding lane-scoped fencing closure and destroyed only after the
	// drain confirms zero outstanding work — on SUCCESS, FAILURE and
	// REFUSAL alike (DrainAndDelete pairs Enter/Exit with
	// mpool.DeleteMPool; no platform mpool change).
	alloc := databranchutils.NewDataBranchStageAllocator(mpool.MustNewZeroNoFixed())
	sub := compile.NewDataBranchFencingRetireSubstrate(
		pu.StorageEngine, alloc.MPool(), pu.TxnClient,
		pu.FileService, pu.LockService, pu.QueryClient, pu.HAKeeperClient,
		pu.UdfService, pu.TaskService, exec,
	)
	if sub == nil {
		alloc.DrainAndDelete()
		return false, false, nil
	}
	sub.WithStageAllocator(alloc).WithGenerationKey(sid)
	// D186(B.4), review F3: the statement root reserves ONE outstanding
	// unit BEFORE the critical section runs (every lane's coverage is
	// then reserved in the dispatching thread, ahead of its goroutine),
	// and the final zero-drain plus mpool destruction ride a detached
	// goroutine - this DROP SNAPSHOT/PITR statement never joins a
	// terminal tail stalled in the lockservice's service-lifetime unlock
	// retry (lockservice/service.go:769 ignores request cancellation),
	// while destruction still observes the same exactly-once
	// zero-outstanding accounting on SUCCESS, FAILURE and REFUSAL alike.
	alloc.Enter()
	defer alloc.DetachDrain()
	return compile.RunDataBranchFencingRetirementCS(
		ctx, sub, txnOp, accountID, tableID,
		func(rctx context.Context) (bool, error) {
			return pkMoveLinkPresenceProbeUnder(rctx, bh, accountID, tableID)
		},
		func(rctx context.Context) (bool, error) {
			return pkMoveLedgerRowPresenceProbeUnder(rctx, bh, accountID, tableID)
		},
	)
}

// pkMoveLinkPresenceProbeUnder is the D170(B)(3) locked-view re-probe for
// one retired address executed through the caller's background executor
// inside the drop's pessimistic transaction: whether any committed update
// link row survives under the address's link-id prefix. A pre-upgrade
// tenant (no link catalog) reads as absent; the presence bit is
// informational for callers and cells.
func pkMoveLinkPresenceProbeUnder(
	ctx context.Context, bh BackgroundExec, accountID uint32, tableID uint64,
) (bool, error) {
	if tableID+1 == 0 {
		// MaxUint64 overflows the half-open upper bound (the delete's own
		// guard decides); the probe reports absent.
		return false, nil
	}
	stmt := fmt.Sprintf(
		"select %s from mo_catalog.%s where %s >= x'%016x' and %s < x'%016x' limit 1",
		catalog.DataBranchUpdateLinkColLinkID, catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, tableID,
		catalog.DataBranchUpdateLinkColLinkID, tableID+1,
	)
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, stmt); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return false, err
	}
	for _, er := range erArray {
		if er.GetRowCount() > 0 {
			return true, nil
		}
	}
	return false, nil
}

// pkMoveLedgerRowPresenceProbeUnder is the D168(B) pre-round gate's
// locked-view ledger probe for one retired address executed through the
// caller's background executor inside the drop's pessimistic transaction:
// whether the wide retention ledger still holds the address's
// row_kind = 0 identity row. A pre-upgrade tenant (no ledger catalog)
// reads as absent.
func pkMoveLedgerRowPresenceProbeUnder(
	ctx context.Context, bh BackgroundExec, accountID uint32, tableID uint64,
) (bool, error) {
	stmt := fmt.Sprintf(
		"select %s from mo_catalog.%s where %s = %d and %s = %d limit 1",
		catalog.DataBranchLinkRetentionColTableID, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	)
	actx := defines.AttachAccountId(ctx, accountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(actx, stmt); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	erArray, err := getResultSet(actx, bh)
	if err != nil {
		return false, err
	}
	for _, er := range erArray {
		if er.GetRowCount() > 0 {
			return true, nil
		}
	}
	return false, nil
}
