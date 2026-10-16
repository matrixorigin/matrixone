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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/spillcleanup"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type metadataAdmissionFS struct {
	fileservice.MutableFileService
	blocked atomic.Bool
	creates int
}

func (fs *metadataAdmissionFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	fs.creates++
	return fs.MutableFileService.CreateFile(ctx, name)
}
func (fs *metadataAdmissionFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked.Load() {
		return os.ErrPermission
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestHashBuildSpillMetadataAdmissionAndHandoff(t *testing.T) {
	baseline := spillcleanup.Used()
	var held []*spillcleanup.Reservation
	t.Cleanup(func() {
		for _, token := range held {
			token.Release()
		}
	})
	for spillcleanup.Used() < spillcleanup.Capacity-1 {
		token, err := spillcleanup.Reserve()
		require.NoError(t, err)
		held = append(held, token)
	}
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	ledger := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	t.Cleanup(ledger.Close)
	require.NoError(t, ledger.SetSpillCaps(1, 1))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &metadataAdmissionFS{MutableFileService: baseFS}
	t.Cleanup(func() {
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
	})
	for i, handoff := range []bool{false, true} {
		generation, err := ledger.OpenGeneration(uint64(i + 1))
		require.NoError(t, err)
		bundle := &spillFileBundle{}
		fd, err := bundle.openFile(proc.Ctx, generation, 0, fault, "metadata-build")
		require.NoError(t, err)
		if handoff {
			_, _, err = bundle.growDisk(0, generation, 1)
			require.NoError(t, err)
			_, err = fd.Write([]byte{1})
			require.NoError(t, err)
			require.NoError(t, bundle.recordDiskWrite(0, fd, 1, 1))
		}
		require.NoError(t, bundle.closeFile(0))
		fault.blocked.Store(true)
		if handoff {
			files, err := bundle.accountedFiles(proc.Ctx, generation)
			require.NoError(t, err)
			bundle.release() // must not release the moved metadata slot
			require.Error(t, files[0].Close())
			files[0].DeferCleanup()
		} else {
			bundle.release()
			require.True(t, bundle.cleanupPending())
			bundle.deferCleanup()
			require.False(t, bundle.cleanupPending())
			require.Zero(t, ledger.SpillDiskUsed(), "empty files still consume metadata admission")
		}
		generation.Close()
		require.EqualValues(t, spillcleanup.Capacity, spillcleanup.Used())
		require.Zero(t, ledger.SpillFDUsed())
		next, err := ledger.OpenGeneration(uint64(i + 10))
		require.NoError(t, err)
		denied := &spillFileBundle{}
		creates := fault.creates
		_, err = denied.openFile(proc.Ctx, next, 0, fault, "must-not-create")
		require.ErrorIs(t, err, spillcleanup.ErrCapacity)
		require.Equal(t, creates, fault.creates)
		require.Zero(t, ledger.SpillFDUsed())
		denied.release()
		next.Close()
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
		require.Zero(t, ledger.SpillDiskUsed())
		_, err = baseFS.OpenFile(context.Background(), "metadata-build")
		require.Error(t, err)
	}
}
