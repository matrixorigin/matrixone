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

//go:build darwin || linux

package process

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type spillRefreshTestFS struct {
	fileservice.FileService
	root func() string
}

func TestMemoryOnlyBudgetPromotesBeforeSpill(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "automatic", true: "explicit"}[explicit], func(t *testing.T) {
			const service = "__process_local_cn__"
			previous, present := executionResourceCNBudgets.LoadAndDelete(service)
			t.Cleanup(func() {
				executionResourceCNBudgets.Delete(service)
				if present {
					executionResourceCNBudgets.Store(service, previous)
				}
			})
			path := t.TempDir()
			local, err := fileservice.NewLocalFS(context.Background(), defines.LocalFileServiceName, path, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { local.Close(context.Background()) })
			var samples atomic.Int32
			fs := &spillRefreshTestFS{FileService: local, root: func() string { samples.Add(1); return path }}
			lim := Limitation{Size: 2 << 20}
			if explicit {
				lim.SpillSize = 8 << 20
			}
			proc := &Process{Base: &BaseProcess{FileService: fs, Lim: lim}}
			admit, err := proc.GetExecutionTransientMemoryAdmission()
			require.NoError(t, err)
			require.Zero(t, samples.Load(), "memory admission must not inspect the spill filesystem")
			memory, err := admit(256)
			require.NoError(t, err)
			generation := proc.Base.executionResourceBudget
			require.NotNil(t, generation)
			t.Cleanup(generation.budget.Close)
			t.Cleanup(generation.Close)
			require.False(t, generation.spillDiskInitialized.Load())
			require.Equal(t, uint64(256), generation.Used())

			child := &Process{Base: &BaseProcess{}}
			require.Error(t, child.BorrowViewSchemaResources(proc, generation), "unpromoted memory authority cannot be shared as a full budget")
			const readers = 8
			results := make(chan *ExecutionResourceGeneration, readers)
			errors := make(chan error, readers)
			var done sync.WaitGroup
			for range readers {
				done.Add(1)
				go func() {
					defer done.Done()
					current, e := proc.GetExecutionResourceBudget()
					results <- current
					errors <- e
				}()
			}
			done.Wait()
			for range readers {
				require.NoError(t, <-errors)
				require.Same(t, generation, <-results)
			}
			require.Equal(t, int32(1), samples.Load(), "promotion must sample once for the generation")
			require.True(t, generation.spillDiskInitialized.Load())
			require.Positive(t, generation.SpillDiskCap())
			if explicit {
				require.Equal(t, uint64(lim.SpillSize), generation.SpillDiskCap())
			}
			require.NoError(t, child.BorrowViewSchemaResources(proc, generation))
			borrowed, err := child.GetExecutionResourceBudget()
			require.NoError(t, err)
			require.Same(t, generation, borrowed)
			disk, err := generation.ReserveSpillDisk(1)
			require.NoError(t, err)
			disk.Release()
			memory.Release()
			require.Zero(t, generation.Used())
		})
	}
}

func TestMemoryOnlyBudgetRejectsClosedGenerationPromotion(t *testing.T) {
	const service = "__process_local_cn__"
	previous, present := executionResourceCNBudgets.LoadAndDelete(service)
	t.Cleanup(func() {
		executionResourceCNBudgets.Delete(service)
		if present {
			executionResourceCNBudgets.Store(service, previous)
		}
	})
	proc := &Process{Base: &BaseProcess{Lim: Limitation{Size: 2 << 20}}}
	admit, err := proc.GetExecutionTransientMemoryAdmission()
	require.NoError(t, err)
	generation := proc.Base.executionResourceBudget
	t.Cleanup(generation.budget.Close)
	memory, err := admit(128)
	require.NoError(t, err)
	generation.Close()
	_, err = admit(1)
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	_, err = proc.GetExecutionResourceBudget()
	require.ErrorIs(t, err, ErrExecutionResourceClosed)
	require.Same(t, generation, proc.Base.executionResourceBudget)
	require.False(t, generation.spillDiskInitialized.Load())
	memory.Release()
	require.Zero(t, generation.Used())
}

func TestMemoryOnlyBudgetPromotionPreservesInvalidatedSpillSample(t *testing.T) {
	const service = "__process_local_cn__"
	previous, present := executionResourceCNBudgets.LoadAndDelete(service)
	t.Cleanup(func() {
		executionResourceCNBudgets.Delete(service)
		if present {
			executionResourceCNBudgets.Store(service, previous)
		}
	})
	path := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(), defines.LocalFileServiceName, path, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	fs := &spillRefreshTestFS{FileService: local, root: func() string { return path }}
	proc := &Process{Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: 2 << 20}}}
	admit, err := proc.GetExecutionTransientMemoryAdmission()
	require.NoError(t, err)
	memory, err := admit(128)
	require.NoError(t, err)
	generation := proc.Base.executionResourceBudget
	t.Cleanup(generation.budget.Close)
	t.Cleanup(generation.Close)
	updates := make(chan error, 1)
	fs.root = func() string {
		updates <- generation.budget.SetSpillCaps(4096, 0)
		return path
	}
	promoted, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.NoError(t, <-updates)
	require.Same(t, generation, promoted)
	require.False(t, generation.budget.spillDiskAutomatic)
	require.Equal(t, uint64(4096), generation.budget.SpillDiskCap())
	require.Equal(t, uint64(4096), generation.SpillDiskCap())
	memory.Release()
	require.Zero(t, generation.Used())
}

func (fs *spillRefreshTestFS) RootPath() string { return fs.root() }

func TestGetExecutionResourceBudgetSamplesSharedSpillLedger(t *testing.T) {
	const service = "__process_local_cn__"
	previous, present := executionResourceCNBudgets.LoadAndDelete(service)
	t.Cleanup(func() {
		executionResourceCNBudgets.Delete(service)
		if present {
			executionResourceCNBudgets.Store(service, previous)
		}
	})
	path := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(), defines.LocalFileServiceName, path, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	var shared *ExecutionResourceBudget
	fs := &spillRefreshTestFS{FileService: local, root: func() string {
		// The filesystem must be sampled after resolving the winning ledger,
		// even for the first process on this CN.
		value, ok := executionResourceCNBudgets.Load(service)
		require.True(t, ok)
		shared = value.(*ExecutionResourceBudget)
		return path
	}}
	first := &Process{Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: 2 << 20}}}
	firstGeneration, err := first.GetExecutionResourceBudget()
	require.NoError(t, err)
	t.Cleanup(shared.Close)
	t.Cleanup(firstGeneration.Close)
	require.Same(t, shared, firstGeneration.budget)
	require.True(t, shared.spillDiskAutomatic)
	require.Positive(t, firstGeneration.SpillDiskCap())

	// A real GetExecutionResourceBudget refresh must validate the ledger around
	// Statfs, not capture an already-sampled value in its callback. The explicit
	// cap reset during RootPath must survive that in-flight automatic refresh.
	fs.root = func() string {
		require.NoError(t, shared.SetSpillCaps(4096, 0))
		return path
	}
	second := &Process{Base: &BaseProcess{FileService: fs, Lim: Limitation{Size: 2 << 20}}}
	secondGeneration, err := second.GetExecutionResourceBudget()
	require.NoError(t, err)
	t.Cleanup(secondGeneration.Close)
	require.Same(t, shared, secondGeneration.budget)
	require.Equal(t, uint64(4096), shared.SpillDiskCap())
	require.Equal(t, uint64(4096), secondGeneration.SpillDiskCap())
	require.False(t, shared.spillDiskAutomatic)
}
