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

package colexec

import (
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestServerIsIsolatedByCNService(t *testing.T) {
	const (
		cn0 = "colexec-isolation-cn0"
		cn1 = "colexec-isolation-cn1"
	)
	runtime.SetupServiceBasedRuntime(cn0, runtime.NewRuntime(metadata.ServiceType_CN, cn0, nil))
	runtime.SetupServiceBasedRuntime(cn1, runtime.NewRuntime(metadata.ServiceType_CN, cn1, nil))

	server0 := NewServer(cn0)
	server1 := NewServer(cn1)
	require.NotSame(t, server0, server1)
	require.Same(t, server0, GetServer(cn0))
	require.Same(t, server1, GetServer(cn1))

	uid := uuid.Must(uuid.NewV7())
	proc0 := &process.Process{}
	proc1 := &process.Process{}
	ch0 := make(process.RemotePipelineInformationChannel)
	ch1 := make(process.RemotePipelineInformationChannel)
	require.NoError(t, server0.PutProcIntoUuidMap(uid, proc0, ch0))
	require.NoError(t, server1.PutProcIntoUuidMap(uid, proc1, ch1))

	server1.RemoveUuidsOwned([]uuid.UUID{uid}, ch1)
	gotProc, gotCh, ok := server0.GetProcByUuid(uid, false)
	require.True(t, ok)
	require.Same(t, proc0, gotProc)
	require.Equal(t, ch0, gotCh)
}

func TestPutProcIntoUuidMapConflictIncludesUuidAndState(t *testing.T) {
	srv := NewServer("")

	t.Run("live", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerProc := &process.Process{}
		ownerCh := make(process.RemotePipelineInformationChannel)
		conflictingCh := make(process.RemotePipelineInformationChannel)
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, conflictingCh)
		})

		require.NoError(t, srv.PutProcIntoUuidMap(uid, ownerProc, ownerCh))
		err := srv.PutProcIntoUuidMap(uid, &process.Process{}, conflictingCh)
		require.Error(t, err)
		require.Contains(t, err.Error(), uid.String())
		require.Contains(t, err.Error(), "state: ready")

		gotProc, gotCh, ok := srv.GetProcByUuid(uid, false)
		require.True(t, ok)
		require.Same(t, ownerProc, gotProc)
		require.Equal(t, ownerCh, gotCh)
	})

	t.Run("attached", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerProc := &process.Process{}
		ownerCh := make(process.RemotePipelineInformationChannel)
		conflictingCh := make(process.RemotePipelineInformationChannel)
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, conflictingCh)
		})

		require.NoError(t, srv.PutProcIntoUuidMap(uid, ownerProc, ownerCh))
		gotProc, gotCh, ok := srv.GetProcByUuid(uid, false)
		require.True(t, ok)
		require.Same(t, ownerProc, gotProc)
		require.Equal(t, ownerCh, gotCh)

		err := srv.PutProcIntoUuidMap(uid, &process.Process{}, conflictingCh)
		require.Error(t, err)
		require.Contains(t, err.Error(), uid.String())
		require.Contains(t, err.Error(), "state: attached")

		// The conflicting owner must not be able to clean up the attached owner.
		srv.RemoveUuidsOwned([]uuid.UUID{uid}, conflictingCh)
		srv.uuidCsChanMap.Lock()
		item, exists := srv.uuidCsChanMap.mp[uid]
		srv.uuidCsChanMap.Unlock()
		require.True(t, exists)
		require.Nil(t, item.proc)
		require.Nil(t, item.ch)
		require.Equal(t, ownerCh, item.ownerCh)
	})

	t.Run("tombstone", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		srv.GetProcByUuid(uid, true)
		conflictingCh := make(process.RemotePipelineInformationChannel)
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, conflictingCh)
		})

		err := srv.PutProcIntoUuidMap(uid, &process.Process{}, conflictingCh)
		require.Error(t, err)
		require.Contains(t, err.Error(), uid.String())
		require.Contains(t, err.Error(), "state: tombstone")

		srv.uuidCsChanMap.Lock()
		_, exists := srv.uuidCsChanMap.mp[uid]
		srv.uuidCsChanMap.Unlock()
		require.False(t, exists)
	})
}

func TestPutProcIntoUuidMapRejectsIncompleteRegistration(t *testing.T) {
	srv := NewServer("")

	uid := uuid.Must(uuid.NewV7())
	err := srv.PutProcIntoUuidMap(
		uid,
		nil,
		make(process.RemotePipelineInformationChannel),
	)
	require.ErrorContains(t, err, "requires a non-nil process")

	err = srv.PutProcIntoUuidMap(uid, &process.Process{}, nil)
	require.ErrorContains(t, err, "requires a non-nil process")

	srv.uuidCsChanMap.Lock()
	_, exists := srv.uuidCsChanMap.mp[uid]
	srv.uuidCsChanMap.Unlock()
	require.False(t, exists)
}

func TestAttachProcByUuidOrWaitPreservesTerminalOwner(t *testing.T) {
	srv := NewServer("")

	t.Run("ready to attached", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerProc := &process.Process{}
		ownerCh := make(process.RemotePipelineInformationChannel)
		require.NoError(t, srv.PutProcIntoUuidMap(uid, ownerProc, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		})

		gotProc, gotCh, state, waiter := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverAttachedNow, state)
		require.Same(t, ownerProc, gotProc)
		require.Equal(t, ownerCh, gotCh)
		require.Nil(t, waiter)

		gotProc, gotCh, state, waiter = srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverAlreadyAttached, state)
		require.Nil(t, gotProc)
		require.Nil(t, gotCh)
		require.Nil(t, waiter)

		srv.uuidCsChanMap.Lock()
		item, exists := srv.uuidCsChanMap.mp[uid]
		srv.uuidCsChanMap.Unlock()
		require.True(t, exists)
		require.Equal(t, remoteReceiverAttached, item.state)
		require.Equal(t, ownerCh, item.ownerCh)
	})

	t.Run("closed before attach", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)
		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		})
		srv.DeleteUuids([]uuid.UUID{uid})

		gotProc, gotCh, state, waiter := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverAlreadyClosed, state)
		require.Nil(t, gotProc)
		require.Nil(t, gotCh)
		require.Nil(t, waiter)
		srv.uuidCsChanMap.Lock()
		_, exists := srv.uuidCsChanMap.mp[uid]
		srv.uuidCsChanMap.Unlock()
		require.False(t, exists)
	})

	t.Run("missing wakes on publication", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)

		gotProc, gotCh, state, waiter := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverMissing, state)
		require.Nil(t, gotProc)
		require.Nil(t, gotCh)
		require.NotNil(t, waiter)

		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		})
		select {
		case <-waiter.Done():
		default:
			t.Fatal("receiver publication did not wake the missing attach waiter")
		}
		waiter.Close()
		waiter.Close()
	})

	t.Run("publication wakes only matching uuid", func(t *testing.T) {
		uid1 := uuid.Must(uuid.NewV7())
		uid2 := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)

		_, _, state1, waiter1 := srv.AttachProcByUuidOrWait(uid1)
		_, _, state2, waiter2 := srv.AttachProcByUuidOrWait(uid2)
		require.Equal(t, RemoteReceiverMissing, state1)
		require.Equal(t, RemoteReceiverMissing, state2)
		require.NotNil(t, waiter1)
		require.NotNil(t, waiter2)
		t.Cleanup(waiter1.Close)
		t.Cleanup(waiter2.Close)

		require.NoError(t, srv.PutProcIntoUuidMap(uid1, &process.Process{}, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid1}, ownerCh)
		})
		select {
		case <-waiter1.Done():
		default:
			t.Fatal("matching receiver publication did not wake its waiter")
		}
		select {
		case <-waiter2.Done():
			t.Fatal("unrelated receiver publication woke the wrong waiter")
		default:
		}

		waiter2.Close()
		srv.uuidCsChanMap.Lock()
		_, leaked := srv.uuidCsChanMap.waiters[uid2]
		srv.uuidCsChanMap.Unlock()
		require.False(t, leaked)
	})

	t.Run("canceling one shared waiter preserves its peer", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)

		_, _, state1, waiter1 := srv.AttachProcByUuidOrWait(uid)
		_, _, state2, waiter2 := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverMissing, state1)
		require.Equal(t, RemoteReceiverMissing, state2)
		require.Same(t, waiter1.state, waiter2.state)

		waiter1.Close()
		waiter1.Close()
		srv.uuidCsChanMap.Lock()
		refs := srv.uuidCsChanMap.waiters[uid].refs
		srv.uuidCsChanMap.Unlock()
		require.Equal(t, 1, refs)

		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		})
		select {
		case <-waiter2.Done():
		default:
			t.Fatal("remaining shared waiter was stranded after its peer canceled")
		}
		waiter2.Close()
	})

	t.Run("canceled generation cannot delete a later generation", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)

		_, _, state1, waiter1 := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverMissing, state1)
		oldDone := waiter1.Done()
		waiter1.Close()

		_, _, state2, waiter2 := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverMissing, state2)
		require.NotEqual(t, oldDone, waiter2.Done())
		waiter1.Close()

		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		t.Cleanup(func() {
			srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		})
		select {
		case <-waiter2.Done():
		default:
			t.Fatal("later receiver wait generation was not published")
		}
		select {
		case <-oldDone:
			t.Fatal("canceled receiver wait generation was spuriously published")
		default:
		}
		waiter2.Close()
	})

	t.Run("owner cleanup before attach publishes closed terminal state", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		ownerCh := make(process.RemotePipelineInformationChannel)

		_, _, state, waiter := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverMissing, state)
		require.NotNil(t, waiter)

		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, ownerCh))
		srv.RemoveUuidsOwned([]uuid.UUID{uid}, ownerCh)
		select {
		case <-waiter.Done():
		default:
			t.Fatal("receiver publication did not wake before owner cleanup")
		}

		gotProc, gotCh, attachState, nextWaiter := srv.AttachProcByUuidOrWait(uid)
		require.Equal(t, RemoteReceiverAlreadyClosed, attachState)
		require.Nil(t, gotProc)
		require.Nil(t, gotCh)
		require.Nil(t, nextWaiter)

		waiter.Close()
		srv.uuidCsChanMap.Lock()
		_, registryLeaked := srv.uuidCsChanMap.mp[uid]
		_, waiterLeaked := srv.uuidCsChanMap.waiters[uid]
		srv.uuidCsChanMap.Unlock()
		require.False(t, registryLeaked)
		require.False(t, waiterLeaked)
	})
}
