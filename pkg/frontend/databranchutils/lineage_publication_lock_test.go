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

package databranchutils

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLineageOwnerLifecycleLockSQLUsesStableCatalogWrite(t *testing.T) {
	require.Equal(t,
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'SNAPSHOT'",
		LineageOwnerLifecycleLockSQL(),
	)
}

func TestLineageOwnerLifecyclePessimisticLockSQLUsesRowLock(t *testing.T) {
	require.Equal(t,
		"select feature_code from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT' for update",
		LineageOwnerLifecyclePessimisticLockSQL(),
	)
}

func TestLineageOwnerLifecycleLockSerializesEmptyProbe(t *testing.T) {
	var (
		stableRow sync.Mutex
		ownerMu   sync.Mutex
		ownerLive bool
	)
	ownerLocked := make(chan struct{})
	allowOwnerCommit := make(chan struct{})
	var allowOwnerCommitOnce sync.Once
	releaseOwner := func() {
		allowOwnerCommitOnce.Do(func() { close(allowOwnerCommit) })
	}
	t.Cleanup(releaseOwner)
	ownerDone := make(chan error, 1)
	go func() {
		locked := false
		err := LockLineageOwnerLifecycle(func(string) error {
			stableRow.Lock()
			locked = true
			close(ownerLocked)
			return nil
		})
		if err == nil {
			// Timestamp selection and row publication both happen while the
			// transaction still owns stableRow.
			<-allowOwnerCommit
			ownerMu.Lock()
			ownerLive = true
			ownerMu.Unlock()
		}
		if locked {
			stableRow.Unlock()
		}
		ownerDone <- err
	}()

	select {
	case <-ownerLocked:
	case <-time.After(time.Second):
		releaseOwner()
		t.Fatal("owner did not acquire the publication lock")
	}

	type alterResult struct {
		observedOwner bool
		err           error
	}
	alterDone := make(chan alterResult, 1)
	go func() {
		err := LockLineageOwnerLifecycle(func(string) error {
			stableRow.Lock()
			return nil
		})
		if err != nil {
			alterDone <- alterResult{err: err}
			return
		}
		defer stableRow.Unlock()
		ownerMu.Lock()
		defer ownerMu.Unlock()
		alterDone <- alterResult{observedOwner: ownerLive}
	}()

	select {
	case <-alterDone:
		releaseOwner()
		t.Fatal("ALTER bypassed an owner publication that had already chosen its timestamp")
	default:
	}
	releaseOwner()
	require.NoError(t, <-ownerDone)
	select {
	case result := <-alterDone:
		require.NoError(t, result.err)
		require.True(t, result.observedOwner, "ALTER must observe the committed owner after waiting")
	case <-time.After(time.Second):
		t.Fatal("ALTER did not resume after owner publication committed")
	}
}
