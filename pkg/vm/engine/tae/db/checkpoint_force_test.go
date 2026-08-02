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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestForceGCKPRetryBackoffIsBoundedAndCancelable(t *testing.T) {
	previous := time.Duration(0)
	for retry := 1; retry < 32; retry++ {
		delay := forceGCKPRetryDelay(retry)
		require.Greater(t, delay, time.Duration(0))
		require.GreaterOrEqual(t, delay, previous)
		require.LessOrEqual(t, delay, forceGCKPRetryMaximumDelay)
		previous = delay
	}
	require.Equal(t, forceGCKPRetryMaximumDelay, previous)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := waitForceGCKPRetry(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), time.Second)
}
