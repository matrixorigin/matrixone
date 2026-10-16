// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package group

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/spillcleanup"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestGroupSpillMetadataAdmissionBeforeCreate(t *testing.T) {
	// Keep only three slots available without creating thousands of test files.
	baseline := spillcleanup.Used()
	var held []*spillcleanup.Reservation
	t.Cleanup(func() {
		for _, token := range held {
			token.Release()
		}
	})
	for spillcleanup.Used() < spillcleanup.Capacity-3 {
		token, err := spillcleanup.Reserve()
		require.NoError(t, err)
		held = append(held, token)
	}
	ledger := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	t.Cleanup(ledger.Close)
	require.NoError(t, ledger.SetSpillCaps(1, 1))
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &terminalRemoveErrorFS{MutableFileService: baseFS}
	fault.blocked.Store(true)
	t.Cleanup(func() {
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
	})
	originalCtx := proc.Ctx
	for i, mode := range []string{"cancel-before-write", "writer-init-error", "nonempty"} {
		generation, err := ledger.OpenGeneration(uint64(i + 1))
		require.NoError(t, err)
		ctr := container{mp: proc.Mp(), budget: generation}
		bucket := &spillBucket{name: "metadata-" + mode}
		ctx, cancel := context.WithCancel(originalCtx)
		proc.Ctx = ctx
		fault.onCreate = nil
		if mode == "cancel-before-write" {
			fault.onCreate = cancel
		}
		if mode == "writer-init-error" {
			ctr.mp = nil
		}
		err = ctr.openSpillBucket(proc, fault, bucket)
		if mode == "writer-init-error" {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
		} else {
			require.NoError(t, err)
			_, err = bucket.writer.Write([]byte{1})
			if mode == "cancel-before-write" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				require.NoError(t, bucket.flushWriter())
			}
			require.Error(t, ctr.freeSpillBucket(bucket))
		}
		ctr.deferSpillCleanup()
		generation.Close()
		cancel()
		require.Zero(t, generation.SpillFDUsed())
		require.Zero(t, generation.Snapshot().Used)
		if mode != "nonempty" {
			require.Zero(t, generation.SpillDiskUsed())
		}
		fd, err := baseFS.OpenFile(originalCtx, bucket.name)
		require.NoError(t, err)
		info, err := fd.Stat()
		require.NoError(t, err)
		require.NoError(t, fd.Close())
		if mode != "nonempty" {
			require.Zero(t, info.Size())
		}
	}
	require.EqualValues(t, spillcleanup.Capacity, spillcleanup.Used())
	require.EqualValues(t, 1, ledger.SpillDiskUsed())
	require.Zero(t, ledger.SpillFDUsed())
	proc.Ctx = originalCtx
	fault.onCreate = nil
	// The unbudgeted compatibility path must obey the same process-wide bound.
	ctr := container{mp: proc.Mp()}
	for range 64 {
		denied := &spillBucket{name: "must-not-create"}
		err := ctr.openSpillBucket(proc, fault, denied)
		if err == nil { // keep the negative-control run cleanup-safe too
			_ = ctr.freeSpillBucket(denied)
			ctr.deferSpillCleanup()
		}
		require.ErrorIs(t, err, spillcleanup.ErrCapacity)
	}
	require.Len(t, fault.names, 3, "exhaustion must reject before CreateFile")
	fault.blocked.Store(false)
	require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
	require.Zero(t, ledger.SpillDiskUsed())
	for _, name := range fault.names {
		_, err := baseFS.OpenFile(originalCtx, name)
		require.Error(t, err)
	}
	bucket := &spillBucket{name: "after-recovery"}
	require.NoError(t, ctr.openSpillBucket(proc, fault, bucket))
	require.NoError(t, ctr.freeSpillBucket(bucket))
}
