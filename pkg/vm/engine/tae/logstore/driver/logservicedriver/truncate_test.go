// Copyright 2021 Matrix Origin
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

package logservicedriver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/require"
)

// TestTruncateFromRemoteClientCreationFailure verifies that truncateFromRemote
// returns the error instead of panicking when the client pool fails to create a
// client on the fly (GetOnFly returns a nil client together with a non
// ErrClientPoolClosed error). This used to dereference a nil *wrappedClient in
// the deferred client.Putback() call and crash the whole TN.
func TestTruncateFromRemoteClientCreationFailure(t *testing.T) {
	var failNow atomic.Bool
	backend := NewMockBackend()

	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       64,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    1,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Nanosecond,
		ClientFactory: func() (logservice.Client, error) {
			// Succeed while the pool is being constructed (it pre-warms a
			// client), then always fail so that GetOnFly returns (nil, err).
			if failNow.Load() {
				return nil, moerr.NewInternalErrorNoCtx("client factory failed")
			}
			return newMockBackendClient(backend), nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool := newClientPool(cfg)
	t.Cleanup(pool.Close)

	// From now on every freshly created client fails.
	failNow.Store(true)

	d := &LogServiceDriver{clientPool: pool}

	require.NotPanics(t, func() {
		_, err := d.truncateFromRemote(context.Background(), 100, 0)
		require.Error(t, err)
	})
}
