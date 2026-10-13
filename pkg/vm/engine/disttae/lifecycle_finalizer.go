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

package disttae

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type LifecycleCatalogWrite func(
	ctx context.Context,
	operator client.TxnOperator,
) error

// FinalizeLifecycleCommit owns one private ordinary TxnOperator. The catalog
// callback writes Dataset or TTL Receipt first; only then is the immutable
// retire control installed, immediately followed by Commit. The operator is
// never returned to a general SQL statement flow.
func FinalizeLifecycleCommit(
	ctx context.Context,
	operator client.TxnOperator,
	store DNStore,
	control *api.LifecycleCommitEntry,
	writeCatalog LifecycleCatalogWrite,
) (err error) {
	defer func() {
		mode := "whole"
		if control != nil &&
			control.RetireMode == api.LifecycleCommitEntry_Rewrite {
			mode = "rewrite"
		}
		result := "success"
		if err != nil {
			result = "error"
			if moerr.IsMoErrCode(err, moerr.ErrTxnUnknown) {
				result = "unknown"
			}
		}
		metricv2.LifecycleFinalTxnCounter.WithLabelValues(mode, result).Inc()
	}()
	if operator == nil || control == nil || writeCatalog == nil {
		return moerr.NewInvalidInput(
			ctx,
			"Lifecycle finalizer input is incomplete",
		)
	}
	rollback := func(cause error) error {
		rollbackCtx, cancelRollback := lifecycleRollbackContext(ctx)
		defer cancelRollback()
		return errors.Join(cause, operator.Rollback(rollbackCtx))
	}
	if err := writeCatalog(ctx, operator); err != nil {
		return rollback(err)
	}
	return finishLifecycleCommit(ctx, operator, store, control)
}

type lifecycleFinalizerOperator interface {
	GetWorkspace() client.Workspace
	Commit(context.Context) error
	Rollback(context.Context) error
}

func finishLifecycleCommit(
	ctx context.Context,
	operator lifecycleFinalizerOperator,
	store DNStore,
	control *api.LifecycleCommitEntry,
) error {
	rollback := func(cause error) error {
		rollbackCtx, cancelRollback := lifecycleRollbackContext(ctx)
		defer cancelRollback()
		return errors.Join(cause, operator.Rollback(rollbackCtx))
	}
	workspace, ok := operator.GetWorkspace().(*Transaction)
	if !ok || workspace == nil {
		return rollback(moerr.NewInternalError(
			ctx,
			"Lifecycle finalizer does not own a disttae workspace",
		))
	}
	workspace.Lock()
	hasCatalogWrite := len(workspace.writes) > 0
	workspace.Unlock()
	if !hasCatalogWrite {
		return rollback(moerr.NewInvalidInput(
			ctx,
			"Lifecycle finalizer has no Dataset or TTL Receipt write",
		))
	}
	if err := workspace.SetLifecycleCommitControl(store, control); err != nil {
		return rollback(err)
	}
	return operator.Commit(ctx)
}
