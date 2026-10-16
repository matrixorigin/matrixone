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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type spillRefreshTestFS struct {
	fileservice.FileService
	root func() string
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
