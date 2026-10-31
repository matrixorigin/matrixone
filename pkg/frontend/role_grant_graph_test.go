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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type roleGraphNoHookExec struct{ BackgroundExec }

type roleGraphNilProcessExec struct{ BackgroundExec }

func (bt *backgroundExecTest) execWithProcessHook(ctx context.Context, sql string, _ backExecProcessHook) error {
	return bt.Exec(ctx, sql)
}

func (roleGraphNilProcessExec) execWithProcessHook(
	ctx context.Context, _ string, hook backExecProcessHook,
) error {
	return hook(ctx, (*process.Process)(nil))
}

func TestLockRoleGrantGraphInBackgroundFailsClosed(t *testing.T) {
	ctx := context.Background()
	require.ErrorContains(t,
		lockRoleGrantGraphInBackground(ctx, roleGraphNoHookExec{}),
		"native background executor")
	require.ErrorContains(t,
		lockRoleGrantGraphInBackground(ctx, roleGraphNilProcessExec{}),
		"background process is unavailable")
}
