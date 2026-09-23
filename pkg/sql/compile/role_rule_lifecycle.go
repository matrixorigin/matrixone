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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type roleRuleLifecycleSnapshotHooks struct {
	acquireReadBarrier func(context.Context) (timestamp.Timestamp, error)
	currentSnapshot    func() timestamp.Timestamp
	updateSnapshot     func(context.Context, timestamp.Timestamp) error
}

func advanceRoleRuleLifecycleSnapshot(ctx context.Context, hooks roleRuleLifecycleSnapshotHooks) error {
	if hooks.acquireReadBarrier == nil || hooks.currentSnapshot == nil || hooks.updateSnapshot == nil {
		return moerr.NewInternalError(ctx, "incomplete role-rule lifecycle snapshot hooks")
	}
	frontier, err := hooks.acquireReadBarrier(ctx)
	if err != nil {
		return err
	}
	if frontier.IsEmpty() {
		return moerr.NewInternalError(ctx, "logtail read barrier returned an empty timestamp")
	}
	if hooks.currentSnapshot().Less(frontier) {
		if err = hooks.updateSnapshot(ctx, frontier); err != nil {
			return err
		}
		if hooks.currentSnapshot().Less(frontier) {
			return moerr.NewInternalError(
				ctx,
				"role-rule lifecycle snapshot did not advance after the logtail read barrier",
			)
		}
	}
	return ctx.Err()
}

func lockRoleRuleLifecycleTable(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
) error {
	if proc == nil || eng == nil {
		return moerr.NewInternalError(ctx, "role-rule lifecycle requires an engine process")
	}
	txnOp := proc.GetTxnOperator()
	if txnOp == nil || !txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		return moerr.NewNotSupported(
			ctx,
			"role rewrite rule changes are not supported outside pessimistic read-committed transactions",
		)
	}
	db, err := eng.Database(ctx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	rel, err := db.Relation(ctx, catalog.MO_ROLE_RULE, nil)
	if err != nil {
		return err
	}
	return lockTableForSnapshotRefresh(ctx, eng, proc, rel, false)
}

// LockRoleRuleLifecycle serializes every frontend writer of mo_role_rule with
// table rename admission. The caller must keep the transaction open after this
// function returns and must perform all role/rule/target decisions in that same
// transaction. The lock and the snapshot refresh intentionally mirror the
// rename admission protocol in alter.go.
func LockRoleRuleLifecycle(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
) error {
	if err := lockRoleRuleLifecycleTable(ctx, eng, proc); err != nil {
		return err
	}
	txnOp := proc.GetTxnOperator()
	return advanceRoleRuleLifecycleSnapshot(ctx, roleRuleLifecycleSnapshotHooks{
		acquireReadBarrier: func(ctx context.Context) (timestamp.Timestamp, error) {
			barrier, ok := getLogtailReadBarrier(eng)
			if !ok {
				return timestamp.Timestamp{}, moerr.NewNotSupported(
					ctx,
					"role rewrite rule changes are not supported without a logtail read barrier",
				)
			}
			return barrier.AcquireLogtailReadBarrier(ctx)
		},
		currentSnapshot: txnOp.SnapshotTS,
		updateSnapshot:  txnOp.UpdateSnapshot,
	})
}
