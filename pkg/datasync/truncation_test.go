// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/assert"
)

func TestTruncation_Create(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()

	t.Run("ok", func(t *testing.T) {
		var syncedLsn atomic.Uint64
		tr := newTruncation(
			*newCommon().withLog(rt.Logger()),
			&syncedLsn,
			withTruncateInterval(time.Millisecond),
		)
		assert.NotNil(t, tr)
		defer tr.Close()
	})

	t.Run("fail", func(t *testing.T) {
		tr := newTruncation(
			*newCommon().withLog(rt.Logger()),
			nil,
			withTruncateInterval(time.Millisecond),
		)
		assert.Nil(t, tr)
	})
}

func TestTruncation_Start(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()

	t.Run("context canceled", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			var syncedLsn atomic.Uint64
			tr := newTruncation(
				*newCommon(),
				&syncedLsn,
				withTruncateInterval(time.Millisecond),
			)
			defer tr.Close()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			go tr.Start(ctx)
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("truncate cancel", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			var syncedLsn atomic.Uint64
			tr := newTruncation(
				*newCommon().withLog(rt.Logger()),
				&syncedLsn,
				withTruncateInterval(time.Millisecond),
			)
			defer tr.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go tr.Start(ctx)
			time.Sleep(time.Millisecond * 50)
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("truncate ok", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			var syncedLsn atomic.Uint64
			tr := newTruncation(
				*newCommon().withLog(rt.Logger()),
				&syncedLsn,
				withTruncateInterval(time.Millisecond),
			)
			defer tr.Close()
			tr.(*truncation).client.client = c.(logservice.StandbyClient)
			trun := tr.(*truncation)
			trun.syncedLSN.Store(10)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go tr.Start(ctx)
			time.Sleep(time.Millisecond * 50)
		}
		logservice.RunClientTest(t, false, nil, fn)
	})
}
