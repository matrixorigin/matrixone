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

package hashbuild

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalBundleRemoveFS struct {
	fileservice.MutableFileService
	blocked atomic.Bool
}

func (fs *terminalBundleRemoveFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked.Load() {
		return errors.New("injected terminal bundle remove failure")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestHashBuildTeardownRetriesWhileOperatorRetained(t *testing.T) {
	for _, boundary := range []string{"reset", "free"} {
		for _, removeError := range []bool{false, true} {
			name := boundary + "/control"
			if removeError {
				name = boundary + "/remove-error"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				t.Cleanup(proc.Free)
				proc.SetMessageBoard(message.NewMessageBoard())
				ctx, cancel := context.WithCancel(context.Background())
				proc.BuildPipelineContext(ctx)
				t.Cleanup(cancel)
				budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
				t.Cleanup(budget.Close)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(generation.Close)
				baseFS, err := proc.GetSpillFileService()
				require.NoError(t, err)
				path := "cached_hashbuild_" + uuid.NewString()
				fd, err := baseFS.CreateFile(context.Background(), path)
				require.NoError(t, err)
				t.Cleanup(func() { _ = fd.Close() })
				disk, err := generation.ReserveSpillDisk(1)
				require.NoError(t, err)
				t.Cleanup(func() { disk.Release() })
				fdToken, err := generation.ReserveSpillFD(1)
				require.NoError(t, err)
				t.Cleanup(func() { fdToken.Release() })
				fault := &terminalBundleRemoveFS{MutableFileService: baseFS}
				fault.blocked.Store(removeError)
				bundle := &spillFileBundle{entries: map[int]*spillFileEntry{
					0: {fs: fault, name: path, created: true, diskToken: disk, file: fd, fdToken: fdToken},
				}}
				op := &HashBuild{ctr: container{spillBundle: bundle}, JoinMapTag: 91904}
				t.Cleanup(func() {
					fault.blocked.Store(false)
					op.Free(proc, false, nil)
					bundle.release()
				})
				cancel()
				if boundary == "reset" {
					op.Reset(proc, true, context.Canceled)
				} else {
					op.Free(proc, true, context.Canceled)
				}
				generation.Close()
				_, statErr := fd.Stat()
				require.Error(t, statErr)
				require.Zero(t, generation.SpillFDUsed())
				if removeError {
					require.EqualValues(t, 1, generation.SpillDiskUsed())
					physical, err := baseFS.OpenFile(context.Background(), path)
					require.NoError(t, err)
					require.NoError(t, physical.Close())
				}
				// Recovery must make progress while the prepared operator stays
				// cached, without another Prepare, Free or Release call.
				fault.blocked.Store(false)
				require.Eventually(t, func() bool { return generation.SpillDiskUsed() == 0 }, 3*time.Second, 10*time.Millisecond)
				require.False(t, bundle.cleanupPending())
				require.Nil(t, op.ctr.spillBundle)
				physical, err := baseFS.OpenFile(context.Background(), path)
				if physical != nil {
					_ = physical.Close()
				}
				require.Error(t, err)
				op.Reset(proc, false, nil)
				op.Free(proc, false, nil)
				require.Zero(t, generation.SpillDiskUsed())
				require.Zero(t, generation.SpillFDUsed())
			})
		}
	}
}

func TestHashBuildReleaseRetriesAfterFilesystemRecovery(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	name := "terminal_hashbuild_" + uuid.NewString()
	fd, err := baseFS.CreateFile(context.Background(), name)
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	token, err := generation.ReserveSpillDisk(1)
	require.NoError(t, err)
	fault := &terminalBundleRemoveFS{MutableFileService: baseFS}
	fault.blocked.Store(true)
	op := NewArgument()
	op.ctr.spillBundle = &spillFileBundle{entries: map[int]*spillFileEntry{
		0: {fs: fault, name: name, created: true, diskToken: token},
	}}
	op.ctr.spillBundle.release() // Error cleanup before Reset/Free; Release is a fallback.
	require.True(t, op.ctr.spillBundle.cleanupPending())
	op.Release()
	generation.Close() // reservations must remain releasable after query teardown
	require.Positive(t, generation.SpillDiskUsed())
	fault.blocked.Store(false)
	require.Eventually(t, func() bool { return generation.SpillDiskUsed() == 0 }, 3*time.Second, 10*time.Millisecond)
	_, err = baseFS.OpenFile(context.Background(), name)
	require.Error(t, err)
}

type blockedBundleRemoveFS struct {
	fileservice.MutableFileService
	blocked       bool
	failRemaining int
	removeCalls   int
}

func (fs *blockedBundleRemoveFS) RemoveFile(ctx context.Context, name string) error {
	fs.removeCalls++
	if fs.blocked || fs.failRemaining > 0 {
		if fs.failRemaining > 0 {
			fs.failRemaining--
		}
		return errors.New("injected bundle remove failure")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestHashBuildHandedOffFileRetriesTransientRemoval(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	name := "hashbuild_handoff_cleanup_" + uuid.NewString()
	fd, err := baseFS.CreateFile(context.Background(), name)
	require.NoError(t, err)
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	defer func() { _ = baseFS.RemoveFile(context.Background(), name) }()
	token, err := generation.ReserveSpillDisk(1)
	require.NoError(t, err)
	fault := &blockedBundleRemoveFS{MutableFileService: baseFS, failRemaining: 2}
	bundle := &spillFileBundle{entries: map[int]*spillFileEntry{
		0: {fs: fault, name: name, created: true, rows: 1, bytes: 1, diskToken: token},
	}}
	defer bundle.release()
	files, err := bundle.accountedFiles(context.Background(), generation)
	require.NoError(t, err)
	require.NotNil(t, files[0])
	require.NoError(t, files[0].Close())
	require.Equal(t, 3, fault.removeCalls)
	require.Zero(t, generation.SpillDiskUsed())
	_, err = baseFS.OpenFile(context.Background(), name)
	require.Error(t, err)
}

func TestSpillBundleReleaseRetainsFailedRemoval(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "remove-error"}[blocked], func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer budget.Close()
			defer generation.Close()
			baseFS, err := proc.GetSpillFileService()
			require.NoError(t, err)
			name := "hashbuild_bundle_cleanup_" + uuid.NewString()
			fd, err := baseFS.CreateFile(context.Background(), name)
			require.NoError(t, err)
			defer fd.Close()
			_, err = fd.Write([]byte{1})
			require.NoError(t, err)
			defer func() { _ = baseFS.RemoveFile(context.Background(), name) }()
			token, err := generation.ReserveSpillDisk(1)
			require.NoError(t, err)
			defer token.Release()
			fdToken, err := generation.ReserveSpillFD(1)
			require.NoError(t, err)
			defer fdToken.Release()
			fault := &blockedBundleRemoveFS{MutableFileService: baseFS, blocked: blocked}
			bundle := &spillFileBundle{entries: map[int]*spillFileEntry{
				0: {fs: fault, name: name, created: true, file: fd, diskToken: token, fdToken: fdToken},
				1: nil,
			}}
			defer func() { fault.blocked = false; bundle.release() }()
			op := &HashBuild{ctr: container{spillBundle: bundle}}
			bundle.release() // Preserve the pre-Reset Prepare retry contract.
			_, statErr := fd.Stat()
			require.Error(t, statErr, "terminal cleanup closes the descriptor even if removal fails")
			require.Zero(t, generation.SpillFDUsed())
			_, handoffErr := bundle.accountedFiles(context.Background(), generation)
			require.ErrorIs(t, handoffErr, process.ErrExecutionSpillReservationInactive)
			if blocked {
				physical, openErr := baseFS.OpenFile(context.Background(), name)
				require.NoError(t, openErr)
				require.NoError(t, physical.Close())
				require.Positive(t, generation.SpillDiskUsed(), "disk ownership must remain while the file exists")
				require.Same(t, bundle, op.ctr.spillBundle)
				require.ErrorContains(t, op.Prepare(proc), "previous hash build spill cleanup is pending")
				require.Same(t, bundle, op.ctr.spillBundle)
				fault.blocked = false
				require.ErrorIs(t, op.Prepare(proc), mpool.ErrAllocationAccountInvalid)
			} else {
				require.ErrorIs(t, op.Prepare(proc), mpool.ErrAllocationAccountInvalid)
			}
			require.Nil(t, op.ctr.spillBundle)
			require.Zero(t, generation.SpillDiskUsed())
			_, err = baseFS.OpenFile(context.Background(), name)
			require.Error(t, err)
		})
	}
}
