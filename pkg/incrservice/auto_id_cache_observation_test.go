// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheUncommittedObservation(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), catalog.System_Account), 10*time.Second)
				defer cancel()
				store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
				s := NewIncrService("", store, Config{EnableAutoIDCache: true}).(*service)
				defer s.Close()
				txn, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer txn.Rollback(ctx)
				cols := []AutoColumn{{TableID: 42, ColName: "id", Step: 1, Offset: 9, CacheSize: 1}}
				require.NoError(t, s.Create(ctx, 42, cols, txn))
				lazy := newLazyPrivateTableCache(42, cols, func(ctx context.Context) (incrTableCache, error) {
					return newTableCache(ctx, "", 42, 0, cols, s.cfg, s.allocator, txn, false)
				})
				defer lazy.close()
				value, err := lazy.currentValue(ctx, 42, "id", store)
				require.NoError(t, err)
				require.Equal(t, uint64(10), value)
				for range 2 {
					value, err := s.CurrentValue(ctx, 42, "id")
					require.NoError(t, err)
					require.Equal(t, uint64(10), value)
				}
				require.Empty(t, store.requests(), "observation must not reserve IDs")
				if commit {
					require.NoError(t, txn.Commit(ctx))
					value, err := s.CurrentValue(ctx, 42, "id")
					require.NoError(t, err)
					require.Equal(t, uint64(10), value)
				} else {
					require.NoError(t, txn.Rollback(ctx))
					_, err := s.CurrentValue(ctx, 42, "id")
					require.Error(t, err)
				}
				require.Empty(t, store.requests())
			})
		})
	}
}
