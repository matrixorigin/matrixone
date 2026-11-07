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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type hashBuildTerminalFS struct {
	*fileservice.LocalETLFS
	blocked  bool
	attempts int
}

var hashBuildUnlinkFailure = errors.New("injected hashbuild terminal unlink failure")

func (f *hashBuildTerminalFS) RemoveFile(ctx context.Context, name string) error {
	f.attempts++
	if f.blocked {
		return hashBuildUnlinkFailure
	}
	return f.LocalETLFS.RemoveFile(ctx, name)
}
func hashBuildTerminalView(t *testing.T) (*hashBuildTerminalFS, fileservice.MutableFileService, string) {
	t.Helper()
	base := t.TempDir()
	local, err := fileservice.NewLocalETLFS(defines.LocalFileServiceName, base)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	fault := &hashBuildTerminalFS{LocalETLFS: local}
	view, err := fileservice.GetSpillFileService(context.Background(), fault, "spill")
	require.NoError(t, err)
	return fault, view, base
}

func TestHashBuildTerminalTeardownAndHandoff(t *testing.T) {
	for _, boundary := range []string{"reset", "free", "release", "handoff"} {
		for _, fail := range []bool{false, true} {
			name := boundary + "/healthy"
			if fail {
				name = boundary + "/failure"
			}
			t.Run(name, func(t *testing.T) {
				fault, view, base := hashBuildTerminalView(t)
				proc := testutil.NewProcess(t)
				proc.SetMessageBoard(message.NewMessageBoard())
				budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
				t.Cleanup(budget.Close)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(generation.Close)
				fd, err := view.CreateFile(context.Background(), "owned")
				require.NoError(t, err)
				_, err = fd.Write([]byte{1})
				require.NoError(t, err)
				disk, err := generation.ReserveSpillDisk(1)
				require.NoError(t, err)
				fdToken, err := generation.ReserveSpillFD(1)
				require.NoError(t, err)
				entry := &spillFileEntry{bucket: 0, fs: view, name: "owned", created: true, file: fd, diskToken: disk, fdToken: fdToken, rows: 1, bytes: 1}
				bundle := &spillFileBundle{entries: map[int]*spillFileEntry{0: entry}}
				op := &HashBuild{ctr: container{spillBundle: bundle}, JoinMapTag: 91904}
				if boundary == "release" {
					op = NewArgument()
					op.ctr.spillBundle = bundle
				}
				fault.blocked = fail
				switch boundary {
				case "reset":
					op.Reset(proc, true, context.Canceled)
				case "free":
					op.Free(proc, true, context.Canceled)
				case "release":
					op.Release()
				case "handoff":
					require.NoError(t, bundle.closeFile(0))
					files, err := bundle.accountedFiles(context.Background(), generation)
					require.NoError(t, err)
					require.Nil(t, bundle.entries)
					if fail {
						require.ErrorIs(t, files[0].Close(), hashBuildUnlinkFailure)
					} else {
						require.NoError(t, files[0].Close())
					}
					_ = files[0].Close()
				}
				generation.Close()
				_, err = fd.Stat()
				require.ErrorIs(t, err, os.ErrClosed)
				require.Zero(t, generation.SpillFDUsed())
				require.Empty(t, bundle.entries)
				if fail {
					require.FileExists(t, filepath.Join(base, "spill", "owned"))
					require.EqualValues(t, 1, generation.SpillDiskUsed())
					require.EqualValues(t, 1, budget.SpillDiskUsed())
					require.False(t, disk.Release(), "a copied terminal token cannot refund a surviving file")
					_, err = view.CreateFile(context.Background(), "new")
					require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
				} else {
					require.NoFileExists(t, filepath.Join(base, "spill", "owned"))
					require.Zero(t, generation.SpillDiskUsed())
				}
				fault.blocked = false
				bundle.release()
				require.Equal(t, 1, fault.attempts)
			})
		}
	}
}

func TestHashBuildTerminalRemovalReleasesWriterCache(t *testing.T) {
	h := newSpillTestHarness(t, 1<<30)
	t.Cleanup(func() { h.close(t); h.generation.Close(); h.budget.Close() })
	fault, view, base := hashBuildTerminalView(t)
	h.op.ctr.spillFS = view
	require.NoError(t, h.op.ctr.writeSpillPayload(h.proc, h.files, 0, []byte{1}, 1, nil))
	bundle := h.op.ctr.spillBundle
	entry := bundle.entries[0]
	require.NotNil(t, entry.cacheToken)
	require.EqualValues(t, 4<<20, h.generation.Used())
	fault.blocked = true
	bundle.release()
	require.Zero(t, h.generation.Used(), "terminal cleanup drops memory/FD ownership even when disk cannot be deleted")
	require.Zero(t, h.generation.SpillFDUsed())
	require.EqualValues(t, 1, h.generation.SpillDiskUsed())
	require.FileExists(t, filepath.Join(base, "spill", entry.name))
	fault.blocked = false
	bundle.release()
	require.Equal(t, 1, fault.attempts)
	require.Zero(t, h.generation.Used())
}

func TestHashBuildTerminalPrepareInvalidInputPreservesGeneration(t *testing.T) {
	for _, configure := range []func(*HashBuild){func(op *HashBuild) { op.NeedBatches = false }, func(op *HashBuild) { op.IsShuffle = true }, func(op *HashBuild) { op.RuntimeFilterSpec = &plan.RuntimeFilterSpec{} }} {
		proc := testutil.NewProcess(t)
		bundle := &spillFileBundle{entries: map[int]*spillFileEntry{0: nil}}
		op := &HashBuild{DirectInput: true, NeedBatches: true}
		configure(op)
		op.ctr.spillBundle = bundle
		atomic.StoreUint32(&op.ctr.terminalPublished, 1)
		op.ctr.runtimeFilterDone = true
		require.ErrorContains(t, op.Prepare(proc), "invalid direct hash build")
		require.Same(t, bundle, op.ctr.spillBundle)
		require.False(t, bundle.released)
		require.EqualValues(t, 1, atomic.LoadUint32(&op.ctr.terminalPublished))
		require.True(t, op.ctr.runtimeFilterDone)
		op.NeedBatches, op.IsShuffle, op.RuntimeFilterSpec = true, false, nil
		require.ErrorIs(t, op.Prepare(proc), mpool.ErrAllocationAccountInvalid)
		require.True(t, bundle.released)
		require.Nil(t, op.ctr.spillBundle)
	}
}
