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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/require"
)

func TestNewClientPoolRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	backend := NewMockBackend()

	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       128,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    2,
		ClientRetryInterval: time.Millisecond,       // 1ms between retries
		ClientRetryDuration: 100 * time.Millisecond, // 100ms budget (enough for any CI)
		ClientFactory: func() (logservice.Client, error) {
			if attempts.Add(1) == 1 {
				return nil, moerr.NewInternalErrorNoCtx("factory boom")
			}
			return newMockBackendClient(backend), nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool := newClientPool(cfg)
	t.Cleanup(pool.Close)

	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.Len(t, pool.clients, 1)
	require.NotNil(t, pool.clients[0])
	require.NotNil(t, pool.clients[0].buf)
}

func TestNewClientPoolPanicsWhenFactoryAlwaysFail(t *testing.T) {
	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       64,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    1,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Nanosecond,
		ClientFactory: func() (logservice.Client, error) {
			return nil, moerr.NewInternalErrorNoCtx("always fail")
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	require.PanicsWithError(t, moerr.NewInternalErrorNoCtx("always fail").Error(), func() {
		newClientPool(cfg)
	})
}
