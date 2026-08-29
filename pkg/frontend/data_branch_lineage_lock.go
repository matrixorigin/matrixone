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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
)

func lockDataBranchLineageOwnerLifecycle(ctx context.Context, bh BackgroundExec) error {
	lockCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	err := databranchutils.LockLineageOwnerLifecycle(func(sql string) error {
		bh.ClearExecResultSet()
		return bh.Exec(lockCtx, sql)
	})
	bh.ClearExecResultSet()
	return err
}

// admitFeatureLimitedLineageOwnerMutation installs the TN-ordered catalog
// frontier before crossing the lifecycle write barrier. An explicit-SI data
// branch transaction keeps its fixed snapshot; its quota check uses a separate
// RC transaction for freshness. Advancing an RC snapshot after the gate write
// can expose both workspace versions of the feature-registry row to later
// quota reads.
func admitFeatureLimitedLineageOwnerMutation(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) error {
	if !featureLimitTxnUsesFixedSnapshot(bh) {
		if err := advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
			return err
		}
	}
	return lockDataBranchLineageOwnerLifecycle(ctx, bh)
}
