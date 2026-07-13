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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestPutProcIntoUuidMapConflictIncludesUuidAndState(t *testing.T) {
	srv := NewServer(nil)

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
		require.Contains(t, err.Error(), "state: live")

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
