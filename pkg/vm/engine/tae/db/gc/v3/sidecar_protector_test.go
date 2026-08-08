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

package gc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSidecarReadProtectorLifecycle(t *testing.T) {
	ctx := context.Background()
	ref := []byte("read-ref")
	expires := time.Now().Add(time.Minute)

	var missing SidecarReadProtector
	require.ErrorContains(t, missing.Register(ctx, ref, []string{"obj"}, expires), "no manager")
	require.ErrorContains(t, missing.Unregister(ctx, ref), "no manager")

	manager := NewSyncProtectionManager()
	protector := SidecarReadProtector{Manager: manager}
	require.ErrorContains(t, protector.Register(ctx, nil, []string{"obj"}, expires), "empty reference")
	require.ErrorContains(t, protector.Register(ctx, ref, []string{""}, expires), "empty object name")

	require.NoError(t, protector.Register(ctx, ref, []string{"obj-a", "obj-b"}, expires))
	require.True(t, manager.IsProtected("obj-a"))
	require.True(t, manager.IsProtected("obj-b"))
	jobID := sidecarReadJobID(ref)
	require.True(t, manager.HasProtection(jobID))

	// Crash replay is idempotent only when the reference retains the exact
	// object set and expiry that were originally protected.
	require.NoError(t, protector.Register(ctx, ref, []string{"obj-a", "obj-b"}, expires))
	require.Error(t, protector.Register(ctx, ref, []string{"different"}, expires))
	require.Error(t, protector.Register(ctx, ref, []string{"obj-a", "obj-b"}, expires.Add(time.Second)))

	require.NoError(t, protector.Unregister(ctx, ref))
	require.NoError(t, protector.Unregister(ctx, ref))
}

func TestSidecarReadProtectionScopeExcludesGC(t *testing.T) {
	ctx := context.Background()
	manager := NewSyncProtectionManager()
	protector := SidecarReadProtector{Manager: manager}
	register, rollback, closeProtection, err := protector.Begin(ctx)
	require.NoError(t, err)
	require.False(t, manager.protectionBarrier.TryLock())
	require.NoError(t, register(ctx, []byte("scoped-read"), []string{"obj"}, time.Now().Add(time.Minute)))
	require.NoError(t, rollback(ctx, []byte("scoped-read")))
	require.False(t, manager.HasProtection(sidecarReadJobID([]byte("scoped-read"))))
	closeProtection()
	closeProtection()
	require.Error(t, register(ctx, []byte("late-read"), []string{"obj"}, time.Now().Add(time.Minute)))
	require.Error(t, rollback(ctx, []byte("late-read")))
	require.True(t, manager.protectionBarrier.TryLock())
	manager.protectionBarrier.Unlock()
}

func TestSidecarReadProtectionRollbackDoesNotDeletePreexistingProtection(t *testing.T) {
	ctx := context.Background()
	manager := NewSyncProtectionManager()
	protector := SidecarReadProtector{Manager: manager}
	ref := []byte("preexisting-read")
	objects := []string{"obj"}
	expires := time.Now().Add(time.Minute)
	require.NoError(t, protector.Register(ctx, ref, objects, expires))

	register, rollback, closeProtection, err := protector.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, register(ctx, ref, objects, expires))
	require.NoError(t, rollback(ctx, ref))
	closeProtection()

	require.True(t, manager.HasProtection(sidecarReadJobID(ref)))
	require.True(t, manager.IsProtected("obj"))
}

func TestSidecarReadProtectorEmptyTableAndGCExclusion(t *testing.T) {
	ctx := context.Background()
	expires := time.Now().Add(time.Minute)

	manager := NewSyncProtectionManager()
	protector := SidecarReadProtector{Manager: manager}
	require.NoError(t, protector.Register(ctx, []byte("empty-table"), nil, expires))
	require.True(t, manager.IsProtected("__sidecar_empty_table__"))

	manager.SetGCRunning(true)
	defer manager.SetGCRunning(false)
	require.Error(t, protector.Register(ctx, []byte("during-gc"), []string{"obj"}, expires))
}
