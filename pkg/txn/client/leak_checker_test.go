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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeakCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ts := newTestTxnSender()

	cc := make(chan struct{})
	c := NewTxnClient(ts,
		WithEnableLeakCheck(
			time.Millisecond*200,
			func(txnID []byte, createAt time.Time, createBy string) {
				close(cc)
			}))
	_, err := c.New(ctx, newTestTimestamp(0))
	assert.Nil(t, err)
	<-cc
	require.NoError(t, c.Close())
}

func TestLeakCheckWithNoLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ts := newTestTxnSender()

	n := 0
	c := NewTxnClient(ts,
		WithEnableLeakCheck(
			time.Millisecond*200,
			func(txnID []byte, createAt time.Time, createBy string) {
				n++
			}))
	op, err := c.New(ctx, newTestTimestamp(0))
	require.NoError(t, err)
	require.NoError(t, op.Rollback(ctx))
	time.Sleep(time.Millisecond * 200 * 3)
	assert.Equal(t, 0, n)
	lc := c.(*txnClient).leakChecker
	lc.Lock()
	assert.Equal(t, 0, len(lc.actives))
	lc.Unlock()
	require.NoError(t, c.Close())
}
