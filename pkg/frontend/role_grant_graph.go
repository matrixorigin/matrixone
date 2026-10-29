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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type catalogLockBackgroundExec interface {
	execWithProcessHook(context.Context, string, backExecProcessHook) error
}

// The hook runs inside the background executor's active transaction. All
// subsequent role and grant lookups must use that same executor until commit.
func lockRoleGrantGraphInBackground(ctx context.Context, bh BackgroundExec) error {
	locker, ok := bh.(catalogLockBackgroundExec)
	if !ok {
		return moerr.NewInternalError(ctx, "role grant graph requires a native background executor")
	}
	return locker.execWithProcessHook(ctx, "select 1", func(ctx context.Context, proc *process.Process) error {
		if proc == nil || proc.GetSessionInfo() == nil {
			return moerr.NewInternalError(ctx, "role grant graph background process is unavailable")
		}
		return compile.LockRoleGrantGraph(ctx, proc.GetSessionInfo().StorageEngine, proc)
	})
}
