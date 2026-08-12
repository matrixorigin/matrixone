// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunViewMetadataRecoveryLoopContinuesAfterFailureAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	calls, failures := 0, 0
	runViewMetadataRecoveryLoop(ctx, ticks, func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("retry next tick")
		}
		cancel()
		return nil
	}, func(error) { failures++ })
	require.Equal(t, 2, calls)
	require.Equal(t, 1, failures)
}

func TestRunViewMetadataRecoveryTickUsesDurableActivationStateAfterRestart(t *testing.T) {
	ctx := context.Background()
	required, started, recovered := 0, 0, 0
	requireRevalidation := func(context.Context) error { required++; return nil }
	startRevalidation := func(context.Context) error { started++; return nil }
	recoverPage := func(context.Context) error { recovered++; return nil }

	require.NoError(t, runViewMetadataRecoveryTick(
		ctx, true, false, requireRevalidation, startRevalidation, recoverPage))
	require.Equal(t, 1, required)
	require.Zero(t, started)
	require.Zero(t, recovered)

	// A restarted CN has no process-local false observation. It still attempts
	// the durable REQUIRED -> SCAN CAS before recovery when its first view of
	// the cluster is enabled.
	require.NoError(t, runViewMetadataRecoveryTick(
		ctx, true, true, requireRevalidation, startRevalidation, recoverPage))
	require.Equal(t, 1, started)
	require.Equal(t, 1, recovered)
}
