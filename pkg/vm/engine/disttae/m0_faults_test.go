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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestM0AlterFaultDispatch(t *testing.T) {
	if fault.Enable() {
		t.Cleanup(func() { fault.Disable() })
	}
	ctx := context.Background()
	name := t.Name()
	require.NoError(t, triggerM0AlterFault(ctx, name))
	require.NoError(t, fault.AddFaultPoint(ctx, name, ":::", "echo", 0, "injected ddl failure", false))
	t.Cleanup(func() {
		_, err := fault.RemoveFaultPoint(ctx, name)
		require.NoError(t, err)
	})
	err := triggerM0AlterFault(ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.ErrorContains(t, err, "injected ddl failure")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, triggerM0AlterFault(canceled, name), context.Canceled)
	require.NoError(t, fault.AddFaultPoint(ctx, name, ":::", "return", 0, "", false))
	require.NoError(t, triggerM0AlterFault(ctx, name))
}
