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

package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestProgrammableFaultInjectorFailsOnlySelectedHit(t *testing.T) {
	injected := errors.New("injected")
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultAfterPayloadPut: FailOnHit(2, injected.Error()),
	})

	require.NoError(t, faults.Inject(context.Background(), FaultAfterPayloadPut))
	require.EqualError(t,
		faults.Inject(context.Background(), FaultAfterPayloadPut),
		injected.Error(),
	)
	require.NoError(t, faults.Inject(context.Background(), FaultAfterPayloadPut))
	require.Equal(t, uint64(3), faults.Hits(FaultAfterPayloadPut))
	require.Zero(t, faults.Hits(FaultAfterManifestPut))
}

func TestProgrammableFaultInjectorCountsConcurrentHits(t *testing.T) {
	const workers = 32
	faults := NewProgrammableFaultInjector(nil)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			require.NoError(t,
				faults.Inject(context.Background(), FaultBeforeSourceRead),
			)
		}()
	}
	wait.Wait()
	require.Equal(t, uint64(workers), faults.Hits(FaultBeforeSourceRead))
}

func TestMOFaultInjectorUsesExistingFaultControlPlane(t *testing.T) {
	fault.Enable()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(
			context.Background(),
			MOFaultPointName(FaultBeforeFinalCommit),
		)
		fault.Disable()
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(),
		MOFaultPointName(FaultBeforeFinalCommit),
		":::",
		"echo",
		17,
		"injected final commit",
		false,
	))

	err := (MOFaultInjector{}).Inject(
		context.Background(),
		FaultBeforeFinalCommit,
	)
	require.ErrorContains(t, err, "injected final commit")
	require.ErrorContains(t, err, "17")
	require.NoError(t, (MOFaultInjector{}).Inject(
		context.Background(),
		FaultBeforeSourceRead,
	))
}
