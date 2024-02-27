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

package rpc

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestHandleGCCache(t *testing.T) {
	now := time.Now()
	expired := now.Add(-MAX_TXN_COMMIT_LATENCY).Add(-time.Second)

	handle := Handle{}
	handle.txnCtxs = common.NewMap[string, *txnContext](10)
	handle.txnCtxs.Store("now", &txnContext{
		deadline: now,
	})
	handle.txnCtxs.Store("expired", &txnContext{
		deadline: expired,
	})
	handle.GCCache(now)

	cnt := 0
	handle.txnCtxs.Range(func(key string, value *txnContext) bool {
		cnt++
		return true
	})

	require.Equal(t, 1, cnt)
	_, ok := handle.txnCtxs.Load("expired")
	require.False(t, ok)
	_, ok = handle.txnCtxs.Load("now")
	require.True(t, ok)
}
