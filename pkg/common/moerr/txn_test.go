// Copyright 2021 - 2022 Matrix Origin
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

package moerr

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMustRollbackTxnPreservesInfrastructurePolicy(t *testing.T) {
	fatal := NewLockConflictNoCtx()
	ordinary := NewInternalErrorNoCtx("ordinary")
	for _, err := range []error{fatal, fmt.Errorf("context: %w", fatal), errors.Join(ordinary, fatal), errors.Join(errors.New("cleanup"), fmt.Errorf("wrapped: %w", fatal))} {
		require.True(t, MustRollbackTxn(err))
	}
	for _, err := range []error{nil, ordinary, errors.New("ordinary"), errors.Join(ordinary, fmt.Errorf("wrapped: %w", ordinary))} {
		require.False(t, MustRollbackTxn(err))
	}
	// The lock-wait-timeout family is statement-scoped, matching MySQL's
	// innodb_rollback_on_timeout=OFF default: an expired lock wait must not doom
	// the owning transaction however it is wrapped or joined, while a deadlock
	// keeps InnoDB's whole-transaction scope.
	for _, err := range []error{
		NewLockWaitTimeoutNoCtx(),
		NewRemoteLockWaitTimeoutNoCtx(),
		fmt.Errorf("statement %d: %w", 1, NewLockWaitTimeoutNoCtx()),
		errors.Join(NewInternalErrorNoCtx("ordinary"), NewRemoteLockWaitTimeoutNoCtx()),
	} {
		require.False(t, MustRollbackTxn(err))
	}
	require.True(t, MustRollbackTxn(NewDeadLockDetectedNoCtx()))
	codes := WholeTxnRollbackCodes()
	codes[0] = 0
	require.NotEqual(t, uint16(0), WholeTxnRollbackCodes()[0])
}
