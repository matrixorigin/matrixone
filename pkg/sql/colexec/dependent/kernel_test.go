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

package dependent

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalKernel struct {
	DemandInput
	prepareErr error
	events     []string
	causes     []error
	failed     []bool
}

func (k *terminalKernel) Prepare(*process.Process) error { return k.prepareErr }
func (k *terminalKernel) Reset(_ *process.Process, failed bool, cause error) {
	k.events = append(k.events, "reset")
	k.causes = append(k.causes, cause)
	k.failed = append(k.failed, failed)
}
func (k *terminalKernel) Free(_ *process.Process, failed bool, cause error) {
	k.events = append(k.events, "free")
	k.causes = append(k.causes, cause)
	k.failed = append(k.failed, failed)
}
func (k *terminalKernel) Release() { k.events = append(k.events, "release") }

func TestNativeKernelTerminalCause(t *testing.T) {
	for _, mode := range []string{"success", "execution_failure", "prepare_failure", "factory_failure"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			cause := errors.New("kernel failed")
			if mode == "success" {
				cause = nil
			}
			op := &terminalKernel{}
			if mode == "prepare_failure" {
				op.prepareErr = cause
			}
			kernel, err := openKernel(proc, func(*process.Process, []*DemandInput) (vm.Operator, error) {
				if mode == "factory_failure" {
					return op, cause
				}
				return op, nil
			}, 0, account, nil)
			if mode == "prepare_failure" || mode == "factory_failure" {
				require.ErrorIs(t, err, cause)
				require.Nil(t, kernel)
			} else {
				require.NoError(t, err)
				defer func() { require.NoError(t, kernel.close(cause)) }()
				require.NoError(t, kernel.close(cause))
				require.NoError(t, kernel.close(cause))
			}
			require.Equal(t, []string{"reset", "free", "release"}, op.events)
			require.Equal(t, []error{cause, cause}, op.causes)
			require.Equal(t, []bool{cause != nil, cause != nil}, op.failed)
			require.Zero(t, budget.Used())
		})
	}
}

func TestSourceEdgeRetiresRoundControlBudget(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, t.Name(), mpool.MustNewZero())
	defer proc.Free()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	defer func() {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	r := &region{proc: proc, budget: budget, execution: &Execution{account: account}}
	for range 3 {
		func() {
			frontier, err := r.newSource(1)
			require.NoError(t, err)
			defer frontier.close()
			one := budget.Used()
			require.Positive(t, one)
			next, err := r.newSource(1)
			require.NoError(t, err)
			defer next.close()
			require.Equal(t, 2*one, budget.Used())
			frontier.close()
			frontier.close()
			require.Equal(t, one, budget.Used())
		}()
		require.Zero(t, budget.Used())
		require.Zero(t, r.charged)
		require.Zero(t, account.Snapshot().Used)
	}
}
