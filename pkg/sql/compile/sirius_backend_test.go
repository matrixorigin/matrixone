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

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type siriusExecutionStub struct {
	run     func(context.Context, *mpool.MPool, *perfcounter.CounterSet, func(*batch.Batch, *perfcounter.CounterSet) error) error
	cleanup func(context.Context, bool) error
}

func (s *siriusExecutionStub) Run(ctx context.Context, mp *mpool.MPool, counters *perfcounter.CounterSet, fill func(*batch.Batch, *perfcounter.CounterSet) error) error {
	return s.run(ctx, mp, counters, fill)
}

func (s *siriusExecutionStub) Cleanup(ctx context.Context) error {
	return s.cleanup(ctx, false)
}

func (s *siriusExecutionStub) CleanupAfterRun(ctx context.Context, err error) error {
	return s.cleanup(ctx, err == nil)
}

func TestSiriusFlightBackendPreservesPreparationOwnership(t *testing.T) {
	require.Nil(t, NewSiriusFlightBackend(nil))
	backend := NewSiriusFlightBackend(&sidecarflight.Runtime{})
	// Invalid preparation fails before any connection is required. Its release
	// callback still runs exactly once. Malformed requests remain terminal even
	// after successful cleanup, just as in the concrete Flight API.
	releases := 0
	execution, err := backend.Prepare(context.Background(), SiriusPrepareRequest{
		Release: func(context.Context) error { releases++; return nil },
	})
	require.ErrorContains(t, err, "query identity and Substrait plan are required")
	require.Nil(t, execution, "a failed Prepare must not return a typed-nil execution")
	require.Equal(t, 1, releases)
	require.False(t, backend.CanFallbackBeforeVisibility(err))
	require.False(t, backend.CanFallbackBeforeVisibility(errors.New("execution failure")))
	require.False(t, backend.CanFallbackBeforeVisibility(nil))
	require.True(t, backend.CanFallbackBeforeVisibility(status.Error(codes.Unavailable, "unavailable")))
}

func TestSiriusBackendExecutionUsesCompilerCleanup(t *testing.T) {
	for _, name := range []string{"success", "consumer error", "canceled request", "cleanup error", "consumer panic"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			consumerErr := errors.New("consumer failed")
			cleanupErr := errors.New("cleanup failed")
			if name == "canceled request" {
				cancel()
			}
			fills, cleanups := 0, 0
			counters := &perfcounter.CounterSet{}
			input := batch.NewWithSize(0)
			defer input.Clean(proc.Mp())
			input.SetRowCount(1)
			execution := &siriusExecutionStub{
				run: func(runCtx context.Context, mp *mpool.MPool, got *perfcounter.CounterSet, fill func(*batch.Batch, *perfcounter.CounterSet) error) error {
					require.Same(t, proc.Mp(), mp)
					require.Same(t, counters, got)
					if err := runCtx.Err(); err != nil {
						return err
					}
					return fill(input, got)
				},
				cleanup: func(cleanupCtx context.Context, succeeded bool) error {
					cleanups++
					require.NoError(t, cleanupCtx.Err(), "cleanup must survive request cancellation")
					_, bounded := cleanupCtx.Deadline()
					require.True(t, bounded)
					require.Equal(t, name == "success" || name == "cleanup error", succeeded)
					if name == "cleanup error" || name == "consumer error" {
						return cleanupErr
					}
					return nil
				},
			}
			c := &Compile{
				proc: proc, counterSet: counters,
				fill: func(got *batch.Batch, _ *perfcounter.CounterSet) error {
					fills++
					require.Same(t, input, got)
					if name == "consumer error" {
						return consumerErr
					}
					if name == "consumer panic" {
						panic(consumerErr)
					}
					return nil
				},
				siriusRead: newSiriusReadOwner(execution, &SiriusRuntime{CleanupTimeout: time.Second}),
			}
			if name == "consumer panic" {
				require.PanicsWithValue(t, consumerErr, func() { _ = c.runSiriusRead(ctx) })
				require.Equal(t, 1, cleanups)
				require.Equal(t, 1, fills)
				return
			}
			err := c.runSiriusRead(ctx)
			require.Equal(t, 1, cleanups)
			if name == "canceled request" {
				require.Zero(t, fills)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.Equal(t, 1, fills)
			}
			switch name {
			case "success":
				require.NoError(t, err)
			case "consumer error":
				require.ErrorIs(t, err, consumerErr)
				require.ErrorIs(t, err, cleanupErr)
			case "cleanup error":
				require.ErrorIs(t, err, cleanupErr)
			}
		})
	}
}
