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
		require.NoError(t, srv.PutProcIntoUuidMap(uid, &process.Process{}, make(chan *process.WrapCs)))
		err := srv.PutProcIntoUuidMap(uid, &process.Process{}, make(chan *process.WrapCs))
		require.Error(t, err)
		require.Contains(t, err.Error(), uid.String())
		require.Contains(t, err.Error(), "state: live")
	})

	t.Run("tombstone", func(t *testing.T) {
		uid := uuid.Must(uuid.NewV7())
		srv.GetProcByUuid(uid, true)
		err := srv.PutProcIntoUuidMap(uid, &process.Process{}, make(chan *process.WrapCs))
		require.Error(t, err)
		require.Contains(t, err.Error(), uid.String())
		require.Contains(t, err.Error(), "state: tombstone")
	})
}
