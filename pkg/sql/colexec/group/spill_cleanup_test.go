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

package group

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type oneShotRemoveErrorFS struct {
	fileservice.MutableFileService
	removeCalls int
}

type switchableRemoveErrorFS struct {
	fileservice.MutableFileService
	blocked bool
}

func (fs *switchableRemoveErrorFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked {
		return errors.New("injected persistent remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func (fs *oneShotRemoveErrorFS) RemoveFile(ctx context.Context, name string) error {
	fs.removeCalls++
	if fs.removeCalls == 1 {
		return errors.New("injected transient remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestGroupCleanupRetriesTransientRemoveFailure(t *testing.T) {
	for _, inject := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		ctr := container{mp: proc.Mp(), budget: budget}
		bkt := &spillBucket{name: "whitebox-group-cleanup"}
		var fs fileservice.MutableFileService = baseFS
		var fault *oneShotRemoveErrorFS
		if inject {
			fault = &oneShotRemoveErrorFS{MutableFileService: baseFS}
			fs = fault
		}
		require.NoError(t, ctr.openSpillBucket(proc, fs, bkt))
		_, err = bkt.writer.Write([]byte("physical-spill-data"))
		require.NoError(t, err)
		require.NoError(t, bkt.flushWriter())
		require.Positive(t, budget.SpillDiskUsed())
		ctr.currentSpillBkt = []*spillBucket{bkt}
		ctr.freeSpillBkts()
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		require.Nil(t, ctr.currentSpillBkt)
		if inject {
			require.Equal(t, 2, fault.removeCalls)
			_, err = baseFS.OpenFile(context.Background(), bkt.name)
			require.Error(t, err, "transient removal error must not orphan the file")
			ctr.freeSpillBkts()
			require.Equal(t, 2, fault.removeCalls)
		} else {
			_, err = baseFS.OpenFile(context.Background(), bkt.name)
			require.Error(t, err)
		}
		proc.Free()
	}
}

func TestDistinctCleanupRetriesTransientRemoveFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	ctr := container{mp: proc.Mp(), budget: budget}
	bkt := &spillBucket{name: "whitebox-distinct-cleanup"}
	fault := &oneShotRemoveErrorFS{MutableFileService: baseFS}
	require.NoError(t, ctr.openSpillBucket(proc, fault, bkt))
	_, err = bkt.writer.Write([]byte("physical-distinct-data"))
	require.NoError(t, err)
	require.NoError(t, bkt.flushWriter())
	require.Positive(t, budget.SpillDiskUsed())
	controller := &distinctSpillController{}
	controller.root[0] = bkt
	controller.close()
	require.Equal(t, 2, fault.removeCalls)
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	_, err = baseFS.OpenFile(context.Background(), bkt.name)
	require.Error(t, err)
	controller.close()
	require.Equal(t, 2, fault.removeCalls)
}

func TestGroupRetainsFailedRemovalForLaterCleanup(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		ctr := container{mp: proc.Mp(), budget: budget}
		bkt := &spillBucket{name: "persistently-blocked-group-spill"}
		fault := &switchableRemoveErrorFS{MutableFileService: baseFS, blocked: true}
		require.NoError(t, ctr.openSpillBucket(proc, fault, bkt))
		_, err = bkt.writer.Write([]byte("persisted-spill-data"))
		require.NoError(t, err)
		require.NoError(t, bkt.flushWriter())
		if distinct {
			controller := &distinctSpillController{ctr: &ctr}
			controller.root[0] = bkt
			controller.close()
		} else {
			ctr.currentSpillBkt = []*spillBucket{bkt}
			ctr.freeSpillBkts()
		}
		require.Len(t, ctr.spillCleanupPending, 1)
		require.Positive(t, budget.SpillDiskUsed())
		file, err := baseFS.OpenFile(context.Background(), bkt.name)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		fault.blocked = false
		ctr.freeSpillBkts()
		require.Empty(t, ctr.spillCleanupPending)
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		_, err = baseFS.OpenFile(context.Background(), bkt.name)
		require.Error(t, err)
		proc.Free()
	}
}
