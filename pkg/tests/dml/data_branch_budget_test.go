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

package dml

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// The active subtest owns the budget; setup and previously admitted scenarios
// must not limit it. The package's go test timeout still bounds the whole suite.
func dataBranchDiffScenarioContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), 240*time.Second)
}

func TestDataBranchDiffScenarioBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup, cancelSetup := context.WithTimeout(t.Context(), 240*time.Second)
		defer cancelSetup()
		// Model a late-admitted scenario after earlier work has exhausted the
		// old fixture-wide allowance, without adding wall time to the UT.
		time.Sleep(241 * time.Second)
		require.ErrorIs(t, setup.Err(), context.DeadlineExceeded)
		first, cancelFirst := dataBranchDiffScenarioContext(t)
		defer cancelFirst()
		require.NoError(t, first.Err())
		deadline, ok := first.Deadline()
		require.True(t, ok)
		require.Equal(t, 240*time.Second, time.Until(deadline))
		<-first.Done()
		require.ErrorIs(t, first.Err(), context.DeadlineExceeded)
		// A timed-out sibling must neither cancel the next scenario nor give
		// it an unbounded context. Explicit cancellation remains effective.
		second, cancelSecond := dataBranchDiffScenarioContext(t)
		defer cancelSecond()
		require.NoError(t, second.Err())
		deadline, ok = second.Deadline()
		require.True(t, ok)
		require.Equal(t, 240*time.Second, time.Until(deadline))
		cancelSecond()
		require.ErrorIs(t, second.Err(), context.Canceled)
	})
}
