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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInternalStatementLockPolicy(t *testing.T) {
	base := context.Background()
	private, option, err := resolveInternalStatementLock(base, executor.StatementOption{}.WithDisableLock())
	require.NoError(t, err)
	require.True(t, option.DisableLock())

	_, inherited, err := resolveInternalStatementLock(private, executor.StatementOption{})
	require.NoError(t, err)
	require.True(t, inherited.DisableLock(), "private child must retain its lock policy")

	shared, required, err := resolveInternalStatementLock(private, executor.StatementOption{}.WithRequireLock())
	require.NoError(t, err)
	require.False(t, required.DisableLock(), "shared catalog mutation must reacquire locking")
	_, nested, err := resolveInternalStatementLock(shared, executor.StatementOption{})
	require.NoError(t, err)
	require.False(t, nested.DisableLock(), "shared mutation's children must not re-inherit private policy")
	_, stillPrivate, err := resolveInternalStatementLock(private, executor.StatementOption{})
	require.NoError(t, err)
	require.True(t, stillPrivate.DisableLock(), "requiring a lock must not alter the parent context")

	_, _, err = resolveInternalStatementLock(base, executor.StatementOption{}.WithDisableLock().WithRequireLock())
	require.Error(t, err)
}
