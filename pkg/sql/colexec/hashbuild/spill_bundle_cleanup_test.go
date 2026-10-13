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
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type blockedBundleRemoveFS struct {
	fileservice.MutableFileService
	blocked bool
}

func (fs *blockedBundleRemoveFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked {
		return errors.New("injected bundle remove failure")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
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
			_, err = fd.Write([]byte{1})
			require.NoError(t, err)
			require.NoError(t, fd.Close())
			defer func() { _ = baseFS.RemoveFile(context.Background(), name) }()
			token, err := generation.ReserveSpillDisk(1)
			require.NoError(t, err)
			fault := &blockedBundleRemoveFS{MutableFileService: baseFS, blocked: blocked}
			bundle := &spillFileBundle{entries: map[int]*spillFileEntry{
				0: {fs: fault, name: name, created: true, diskToken: token},
			}}
			defer func() { fault.blocked = false; bundle.release() }()
			op := &HashBuild{ctr: container{spillBundle: bundle}}
			op.cleanupSpillFiles(proc)
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
			}
			require.Nil(t, op.ctr.spillBundle)
			require.Zero(t, generation.SpillDiskUsed())
			_, err = baseFS.OpenFile(context.Background(), name)
			require.Error(t, err)
		})
	}
}
