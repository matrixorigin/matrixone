// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCloseWithoutFreeWithContextLogsBeforeSliceRelease(t *testing.T) {
	reuse.RunReuseTests(func() {
		core, logs := observer.New(zap.DebugLevel)
		logger := log.GetServiceLogger(
			zap.New(core),
			metadata.ServiceType_CN,
			"close-debug",
		)
		id := []byte("close-debug")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		defer reuse.Free(txn, nil)

		bind := pb.LockTable{Group: 0, Table: 1}
		table := &retryableUnlockTestTable{bind: bind}
		require.NoError(t, txn.lockAdded(
			bind.Group,
			bind,
			[][]byte{[]byte("key")},
			pb.LockOptions{},
			logger,
		))

		done := make(chan error, 1)
		go func() {
			done <- txn.closeWithoutFreeWithContext(
				context.Background(),
				id,
				timestamp.Timestamp{},
				func(pb.LockTable) (lockTable, error) {
					return table, nil
				},
				logger,
			)
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("transaction close blocked while logging a released cowSlice")
		}
		require.Equal(t, 1, logs.FilterMessage("txn unlock table completed").Len())
		require.Empty(t, txn.lockHolders)
		require.Equal(t, uint64(1), fsp.releaseV.Load())
	})
}
