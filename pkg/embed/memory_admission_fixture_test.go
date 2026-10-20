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

package embed

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestWithTestMemoryBudgetRequiresTesting(t *testing.T) {
	c, err := NewCluster(withFunctionalTestMemoryAdmission())
	require.Nil(t, c)
	require.ErrorContains(t, err, "requires WithTesting")
}

func TestWithTestMemoryBudgetRemainsFinite(t *testing.T) {
	c := &cluster{}
	withFunctionalTestMemoryAdmission()(c)
	b := c.options.testMemoryBudget
	require.NotSame(t, system.DefaultMemoryAdmissionBudget(), b)
	require.Equal(t, uint64(16<<30), b.Snapshot().Domain.Limit)
	_, ok := b.TryReserve(64 << 10)
	require.True(t, ok)
	b.Release(64 << 10)
	_, ok = b.TryReserve(16 << 30)
	require.False(t, ok, "fixture isolation must not disable finite admission")
}

func TestWithTestMemoryBudgetIsOptIn(t *testing.T) {
	op := &operator{sid: t.Name(), serviceType: metadata.ServiceType_CN}
	c := &cluster{services: []*operator{op}, startFn: func(*operator) error { return nil }}
	WithTesting()(c)
	require.NoError(t, c.doStartLocked(0))
	require.Empty(t, c.testMemoryCleanups, "WithTesting alone must not select a synthetic owner")
	cleanup, err := process.SetServiceMemoryAdmissionBudgetForTesting(op.sid, system.DefaultMemoryAdmissionBudget())
	require.NoError(t, err)
	cleanup()
	require.NoError(t, c.Close())
}

func TestWithTestMemoryBudgetLifecycle(t *testing.T) {
	for _, failStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "startup-failure"}[failStart], func(t *testing.T) {
			c := &cluster{}
			WithTesting()(c)
			withFunctionalTestMemoryAdmission()(c)
			op := &operator{sid: t.Name(), serviceType: metadata.ServiceType_CN}
			c.services = []*operator{op}
			startErr := errors.New("controlled start failure")
			c.startFn = func(*operator) error {
				_, err := process.SetServiceMemoryAdmissionBudgetForTesting(op.sid, c.options.testMemoryBudget)
				require.ErrorIs(t, err, process.ErrExecutionResourceInvalid, "owner installed before CN starts")
				if failStart {
					return startErr
				}
				return nil
			}
			for range 2 { // A closed cluster can reinstall ownership on restart.
				err := c.doStartLocked(0)
				if failStart {
					require.ErrorIs(t, err, startErr)
				} else {
					require.NoError(t, err)
				}
				require.False(t, c.CloseComplete(), "memory owners are cleanup obligations")
				require.NoError(t, c.Close())
				require.True(t, c.CloseComplete())
				cleanup, err := process.SetServiceMemoryAdmissionBudgetForTesting(op.sid, c.options.testMemoryBudget)
				require.NoError(t, err)
				cleanup()
			}
		})
	}
}

func TestWithTestMemoryBudgetRetainedOnIncompleteClose(t *testing.T) {
	c := &cluster{}
	WithTesting()(c)
	withFunctionalTestMemoryAdmission()(c)
	op := &operator{sid: t.Name(), serviceType: metadata.ServiceType_CN}
	c.services = []*operator{op}
	c.startFn = func(*operator) error { return nil }
	require.NoError(t, c.doStartLocked(0))
	service := &closeTrackingService{closeErr: errors.New("controlled drain failure")}
	op.reset.svc = service
	require.Error(t, c.Close())
	require.False(t, c.CloseComplete())
	_, err := process.SetServiceMemoryAdmissionBudgetForTesting(op.sid, c.options.testMemoryBudget)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid, "live CN retains its ledger")
	service.closeErr = nil
	require.NoError(t, c.Close())
	require.True(t, c.CloseComplete())
}
