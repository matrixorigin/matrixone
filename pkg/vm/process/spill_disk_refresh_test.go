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

package process

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAutomaticSpillDiskCapReleaseDuringSample(t *testing.T) {
	const gib = uint64(1 << 30)
	budget := MustNewExecutionResourceBudget(1024, 1024)
	t.Cleanup(budget.Close)
	require.NoError(t, budget.refreshAutomaticSpillDiskCap(func() uint64 { return 256 * gib }))
	first, err := budget.openProcessGeneration(1, 1024, 0)
	require.NoError(t, err)
	t.Cleanup(first.Close)
	second, err := budget.openProcessGeneration(2, 1024, 0)
	require.NoError(t, err)
	t.Cleanup(second.Close)
	retiring, err := first.ReserveSpillDisk(100 * gib)
	require.NoError(t, err)
	t.Cleanup(func() { retiring.Release() })
	live, err := second.ReserveSpillDisk(28 * gib)
	require.NoError(t, err)
	t.Cleanup(func() { live.Release() })

	sampled := make(chan struct{})
	resume := make(chan struct{})
	finished := make(chan struct{})
	result := make(chan error, 1)
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(func() {
		unblock()
		<-finished
	})
	go func() {
		defer close(finished)
		result <- budget.refreshAutomaticSpillDiskCap(func() uint64 {
			// Both files were already written when the filesystem was sampled.
			close(sampled)
			<-resume
			return 128 * gib
		})
	}()
	<-sampled
	// Physical removal precedes ledger release. It restores 100 GiB after the
	// sample was taken; cleanup must not wait for its publication.
	require.True(t, retiring.Release())
	unblock()
	require.NoError(t, <-result)
	require.Equal(t, 192*gib, budget.SpillDiskCap())

	// A fresh sample must not inherit a falsely tightened active envelope.
	require.NoError(t, budget.refreshAutomaticSpillDiskCap(func() uint64 { return 228 * gib }))
	require.Equal(t, 192*gib, budget.SpillDiskCap())
	require.NoError(t, live.Grow(100*gib))
	require.Equal(t, 128*gib, budget.SpillDiskUsed())
}

func TestAutomaticSpillDiskCapInvalidatedSamples(t *testing.T) {
	const gib = uint64(1 << 30)
	for _, name := range []string{"reserve", "grow", "reconcile", "release_and_reserve", "cap_override", "newer_sample", "close"} {
		t.Run(name, func(t *testing.T) {
			budget := MustNewExecutionResourceBudget(1024, 1024)
			t.Cleanup(budget.Close)
			require.NoError(t, budget.refreshAutomaticSpillDiskCap(func() uint64 { return 256 * gib }))
			generation, err := budget.openProcessGeneration(1, 1024, 0)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			disk, err := generation.ReserveSpillDisk(128 * gib)
			require.NoError(t, err)
			t.Cleanup(func() { disk.Release() })
			wantCap := uint64(192 * gib)
			err = budget.refreshAutomaticSpillDiskCap(func() uint64 {
				// These transitions occur after sampling, before publication. The
				// low sample would otherwise tighten the cap. Calling transitions
				// here also proves sampling does not own the admission/cleanup lock.
				switch name {
				case "reserve":
					extra, err := generation.ReserveSpillDisk(gib)
					require.NoError(t, err)
					t.Cleanup(func() { extra.Release() })
				case "grow":
					require.NoError(t, disk.Grow(gib))
				case "reconcile":
					ok, err := disk.ReconcileDown(28 * gib)
					require.NoError(t, err)
					require.True(t, ok)
				case "release_and_reserve":
					require.True(t, disk.Release())
					replacement, err := generation.ReserveSpillDisk(128 * gib)
					require.NoError(t, err)
					t.Cleanup(func() { replacement.Release() })
				case "cap_override":
					wantCap = 160 * gib
					require.NoError(t, budget.SetSpillCaps(wantCap, 0))
				case "newer_sample":
					wantCap = 168 * gib
					require.NoError(t, budget.refreshAutomaticSpillDiskCap(func() uint64 { return 96 * gib }))
				case "close":
					budget.Close()
				}
				return 64 * gib
			})
			if name == "close" {
				require.ErrorIs(t, err, ErrExecutionResourceClosed)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, wantCap, budget.SpillDiskCap())
		})
	}
}

func TestAutomaticSpillDiskCapUnavailableSample(t *testing.T) {
	var nilBudget *ExecutionResourceBudget
	require.ErrorIs(t, nilBudget.refreshAutomaticSpillDiskCap(func() uint64 { return 1 }), ErrExecutionResourceInvalid)
	budget := MustNewExecutionResourceBudget(1024, 1024)
	t.Cleanup(budget.Close)
	require.ErrorIs(t, budget.refreshAutomaticSpillDiskCap(nil), ErrExecutionResourceInvalid)
	before := budget.Snapshot()
	require.NoError(t, budget.refreshAutomaticSpillDiskCap(func() uint64 { return 0 }))
	require.Equal(t, before, budget.Snapshot())
	budget.Close()
	require.ErrorIs(t, budget.refreshAutomaticSpillDiskCap(func() uint64 {
		t.Fatal("closed budget must not sample the filesystem")
		return 0
	}), ErrExecutionResourceClosed)
}
