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

package hashbuild

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTerminalBudgetError(t *testing.T) {
	t.Run("nil and unrelated pass through", func(t *testing.T) {
		require.NoError(t, TerminalBudgetError(context.Background(), nil))
		other := errors.New("other")
		require.Same(t, other, TerminalBudgetError(context.Background(), other))
	})

	for _, tc := range []struct {
		name      string
		component process.HashBuildBudgetComponent
		want      []string
	}{
		{"memory", process.HashBuildBudgetComponentMemory, []string{"memory", "requested=3", "used=5", "limit=7", "build width", "processLimitationSize", "join_spill_mem", "recovery headroom"}},
		{"spill disk", process.HashBuildBudgetComponentSpillDisk, []string{"spill disk", "requested=3", "used=5", "limit=7", "processLimitationSpillSize"}},
		{"spill fd", process.HashBuildBudgetComponentSpillFD, []string{"spill file descriptor", "requested=3", "used=5", "limit=7", "open-file limit"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := TerminalBudgetError(context.Background(), &process.HashBuildBudgetError{
				Kind:      process.HashBuildBudgetErrorAdmission,
				Component: tc.component,
				Requested: 3,
				Used:      5,
				Cap:       7,
				Message:   process.ErrHashBuildBudgetAdmission.Error() + ": requested=3 used=5 cap=7",
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
			require.NotErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
			for _, want := range tc.want {
				require.Contains(t, err.Error(), want)
			}
			require.NotContains(t, err.Error(), process.ErrHashBuildBudgetAdmission.Error())
		})
	}

	t.Run("synthetic admission keeps reason without fake counters", func(t *testing.T) {
		err := TerminalBudgetError(context.Background(), &process.HashBuildBudgetError{
			Kind:    process.HashBuildBudgetErrorAdmission,
			Message: "join spill cannot make progress at depth 8: " + process.ErrHashBuildBudgetAdmission.Error(),
		})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
		require.Contains(t, err.Error(), "join spill cannot make progress at depth 8")
		require.NotContains(t, err.Error(), "requested=0")
		require.NotContains(t, err.Error(), process.ErrHashBuildBudgetAdmission.Error())
	})

	t.Run("resource admission keeps spill depth context", func(t *testing.T) {
		err := TerminalBudgetError(context.Background(), &process.HashBuildBudgetError{
			Kind:      process.HashBuildBudgetErrorAdmission,
			Component: process.HashBuildBudgetComponentMemory,
			Requested: 3,
			Used:      5,
			Cap:       7,
			Message:   "join spill cannot make progress at depth 3",
		})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
		require.Contains(t, err.Error(), "hash build memory budget exceeded")
		require.Contains(t, err.Error(), "join spill cannot make progress at depth 3")
		require.NotContains(t, err.Error(), process.ErrHashBuildBudgetAdmission.Error())
	})

	t.Run("raw admission is generic resource exhaustion", func(t *testing.T) {
		err := TerminalBudgetError(context.Background(), process.ErrHashBuildBudgetAdmission)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
		require.Contains(t, err.Error(), "hash build resource budget exceeded")
		require.NotContains(t, err.Error(), process.ErrHashBuildBudgetAdmission.Error())
	})

	t.Run("physical capacity is terminal resource exhaustion", func(t *testing.T) {
		err := TerminalBudgetError(
			context.Background(), mpool.ErrAllocationAccountCapacity)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM))
		require.Contains(t, err.Error(), "hash build memory budget exceeded")
		require.Contains(t, err.Error(), "processLimitationSize")
	})

	t.Run("physical lifecycle failure stays fatal", func(t *testing.T) {
		joined := errors.Join(
			mpool.ErrAllocationAccountCapacity,
			mpool.ErrAllocationAccountSealed,
		)
		require.Same(t, joined,
			TerminalBudgetError(context.Background(), joined))
	})

	for _, lifecycle := range []error{
		process.ErrHashBuildBudgetClosed,
		process.ErrHashBuildBudgetInvalid,
		process.ErrHashBuildCeilingMissing,
	} {
		t.Run(lifecycle.Error(), func(t *testing.T) {
			joined := errors.Join(process.ErrHashBuildBudgetAdmission, lifecycle)
			require.Same(t, joined, TerminalBudgetError(context.Background(), joined))
		})
	}
}
