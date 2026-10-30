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

package spillutil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type joinTerminalFS struct {
	*fileservice.LocalETLFS
	blocked  bool
	attempts int
}

var joinUnlinkFailure = errors.New("injected join terminal unlink failure")

func (f *joinTerminalFS) RemoveFile(ctx context.Context, name string) error {
	f.attempts++
	if f.blocked {
		return joinUnlinkFailure
	}
	return f.LocalETLFS.RemoveFile(ctx, name)
}

func joinTerminalFixture(t *testing.T) (*process.Process, *joinTerminalFS, fileservice.MutableFileService, *process.ExecutionResourceGeneration, string) {
	t.Helper()
	base := t.TempDir()
	local, err := fileservice.NewLocalETLFS(defines.LocalFileServiceName, base)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	fault := &joinTerminalFS{LocalETLFS: local}
	proc := testutil.NewProcess(t, testutil.WithFileService(fault))
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	t.Cleanup(budget.Close)
	g, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(g.Close)
	view, err := proc.GetSpillFileService()
	require.NoError(t, err)
	return proc, fault, view, g, base
}

func TestJoinTerminalOwnershipBoundaries(t *testing.T) {
	for _, boundary := range []string{"writer", "handoff", "reader", "finish", "engine", "rejected"} {
		for _, fail := range []bool{false, true} {
			name := boundary + "/healthy"
			if fail {
				name = boundary + "/failure"
			}
			t.Run(name, func(t *testing.T) {
				proc, fault, view, g, base := joinTerminalFixture(t)
				writer := BucketWriter{Name: "owned", Budget: g}
				require.NoError(t, writeBucketPayload(proc, []byte("real-spill-data"), 1, &writer, nil))
				var cleanupErr error
				if boundary == "writer" {
					fault.blocked = fail
					cleanupErr = writer.Close()
					require.NoError(t, writer.Close())
				} else {
					file, err := writer.handOffSpillFile(proc.Ctx)
					require.NoError(t, err)
					require.False(t, writer.Created())
					require.Nil(t, writer.diskReservation)
					fault.blocked = fail
					switch boundary {
					case "handoff":
						cleanupErr = file.Close()
						_ = file.Close()
					case "reader":
						r := BucketReader{}
						require.NoError(t, r.ResetForSpillFile(proc, g, file))
						cleanupErr = r.Close()
						require.Nil(t, r.spillFile)
						require.NoError(t, r.Close())
					case "finish":
						e := &SpillEngine{}
						require.NoError(t, e.probeReader.ResetForSpillFile(proc, g, file))
						cleanupErr = e.FinishBucket()
						require.NoError(t, e.FinishBucket())
					case "engine":
						e := &SpillEngine{buckets: []SpillBucket{{BuildFd: file, BuildRows: 1}}}
						e.Cleanup(proc)
						e.Cleanup(proc)
						require.Empty(t, e.buckets)
					case "rejected":
						jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
						jm.IncRef(1)
						defer jm.FreeMemory()
						jm.SetRowCount(2)
						require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{Files: []*message.SpillFile{file}, BudgetRef: g}))
						_, _, cleanupErr = TakeSpillBuildPayload(proc, jm)
						require.ErrorContains(t, cleanupErr, "row count")
					}
				}
				g.Close()
				if fail {
					if boundary != "engine" {
						require.ErrorIs(t, cleanupErr, joinUnlinkFailure)
					}
					require.EqualValues(t, len("real-spill-data"), g.SpillDiskUsed())
					require.FileExists(t, filepath.Join(base, defines.SpillFileServiceName, "owned"))
					_, err := view.CreateFile(context.Background(), "next")
					require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
				} else {
					if boundary != "rejected" {
						require.NoError(t, cleanupErr)
					}
					require.Zero(t, g.SpillDiskUsed())
					require.NoFileExists(t, filepath.Join(base, defines.SpillFileServiceName, "owned"))
				}
				require.Zero(t, g.SpillFDUsed())
				require.Equal(t, 1, fault.attempts)
				fault.blocked = false
			})
		}
	}
}

func TestJoinTerminalReaderRejectsInvalidFileAndReportsCleanup(t *testing.T) {
	proc, fault, view, g, _ := joinTerminalFixture(t)
	w := BucketWriter{Name: "invalid", Budget: g}
	require.NoError(t, writeBucketPayload(proc, []byte{1}, 0, &w, nil))
	file, err := w.handOffSpillFile(proc.Ctx)
	require.NoError(t, err)
	fault.blocked = true
	r := BucketReader{}
	err = r.ResetForSpillFile(nil, nil, file)
	require.ErrorContains(t, err, "corrupted spill file row count")
	require.ErrorIs(t, err, joinUnlinkFailure)
	require.Nil(t, r.spillFile)
	require.Zero(t, g.SpillFDUsed())
	require.EqualValues(t, 1, g.SpillDiskUsed())
	_, err = view.CreateFile(context.Background(), "new")
	require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
	require.NoError(t, r.Close())
	require.Equal(t, 1, fault.attempts)
}

func TestJoinTerminalReaderReplacementCleansBothOwnersOnError(t *testing.T) {
	for _, incoming := range []bool{false, true} {
		t.Run(map[bool]string{false: "no incoming file", true: "incoming file"}[incoming], func(t *testing.T) {
			proc, fault, view, g, _ := joinTerminalFixture(t)
			reader := BucketReader{}
			t.Cleanup(func() { _ = reader.Close() })
			makeFile := func(name string) *message.SpillFile {
				writer := BucketWriter{Name: name, Budget: g}
				t.Cleanup(func() { _ = writer.Close() })
				require.NoError(t, writeBucketPayload(proc, []byte("payload"), 1, &writer, nil))
				file, err := writer.handOffSpillFile(proc.Ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = file.Close() })
				return file
			}
			require.NoError(t, reader.ResetForSpillFile(proc, g, makeFile("old")))
			var next *message.SpillFile
			if incoming {
				next = makeFile("next")
			}
			charged := g.SpillDiskUsed()
			fault.blocked = true
			require.ErrorIs(t, reader.ResetForSpillFile(proc, g, next), joinUnlinkFailure)
			require.Nil(t, reader.spillFile)
			require.Nil(t, reader.fd)
			require.Zero(t, g.SpillFDUsed())
			require.Equal(t, charged, g.SpillDiskUsed(), "failed replacement must not refund either file")
			wantRemovals := 1
			if incoming {
				wantRemovals++
			}
			require.Equal(t, wantRemovals, fault.attempts)
			require.NoError(t, reader.Close())
			require.Equal(t, wantRemovals, fault.attempts)
			_, err := view.CreateFile(context.Background(), "after-replacement")
			require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
		})
	}
}

func TestJoinTerminalReaderFailedOpenReleasesFDAndDisk(t *testing.T) {
	proc, fault, view, g, _ := joinTerminalFixture(t)
	writer := BucketWriter{Name: "missing-before-open", Budget: g}
	t.Cleanup(func() { _ = writer.Close() })
	require.NoError(t, writeBucketPayload(proc, []byte("payload"), 1, &writer, nil))
	file, err := writer.handOffSpillFile(proc.Ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	require.NoError(t, view.RemoveFile(proc.Ctx, "missing-before-open"))
	reader := BucketReader{}
	t.Cleanup(func() { _ = reader.Close() })
	require.Error(t, reader.ResetForSpillFile(proc, g, file))
	require.Nil(t, reader.spillFile)
	require.Nil(t, reader.fdReservation)
	require.Zero(t, g.SpillFDUsed())
	require.Zero(t, g.SpillDiskUsed(), "confirmed missing file must release its disk reservation")
	require.Equal(t, 2, fault.attempts, "failed open still closes its incoming owner once")
	require.NoError(t, reader.Close())
}

func TestJoinTerminalWriterDoesNotRetainQuery(t *testing.T) {
	proc, fault, _, g, base := joinTerminalFixture(t)
	engineRef, queryRef := func() (weak.Pointer[SpillEngine], weak.Pointer[[1024]byte]) {
		e := &SpillEngine{cfg: SpillEngineConfig{Budget: g}}
		query := new([1024]byte)
		e.probeKeyEval = func(*batch.Batch) ([]*vector.Vector, error) { runtime.KeepAlive(query); return nil, nil }
		writers := e.makeBucketWriters("detached")
		w := &writers[0]
		require.NoError(t, writeBucketPayload(proc, []byte("real-spill-data"), 1, w, nil))
		fault.blocked = true
		require.ErrorIs(t, w.Close(), joinUnlinkFailure)
		e.Cleanup(proc)
		return weak.Make(e), weak.Make(query)
	}()
	require.Eventually(t, func() bool { runtime.GC(); return engineRef.Value() == nil && queryRef.Value() == nil }, 5*time.Second, 10*time.Millisecond)
	g.Close()
	require.EqualValues(t, len("real-spill-data"), g.SpillDiskUsed())
	require.Zero(t, g.SpillFDUsed())
	entries, err := os.ReadDir(filepath.Join(base, defines.SpillFileServiceName))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestJoinTerminalScatterPreservesUpstreamAndCleanupErrors(t *testing.T) {
	proc, fault, _, g, base := joinTerminalFixture(t)
	e := newExactTestSpillEngine(t, SpillEngineConfig{Budget: g, NeedsProbeForEmptyBuild: true})
	t.Cleanup(func() { e.Cleanup(proc) })
	e.buckets = []SpillBucket{{}}
	e.scatterCoalesceDisabled = true
	input := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	defer input.Clean(proc.Mp())
	fault.blocked = true
	upstream := errors.New("injected upstream error")
	calls := 0
	err := e.ScatterProbeTable(proc, func() (*batch.Batch, error) {
		calls++
		if calls == 1 {
			return input, nil
		}
		return nil, upstream
	}, process.NewAnalyzer(0, false, false, "test"), makeTestEvalKeysFn())
	require.ErrorIs(t, err, upstream)
	require.ErrorIs(t, err, joinUnlinkFailure)
	require.Positive(t, g.SpillDiskUsed())
	require.Zero(t, g.SpillFDUsed())
	require.Equal(t, 1, fault.attempts)
	entries, err := os.ReadDir(filepath.Join(base, defines.SpillFileServiceName))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	e.Cleanup(proc)
	require.Equal(t, 1, fault.attempts)
}

func TestJoinTerminalRespillQueueFailureCleansTemporaryOwners(t *testing.T) {
	proc, fault, view, g, _ := joinTerminalFixture(t)
	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i)
	}
	build := makeInt32Batch(proc, values)
	defer build.Clean(proc.Mp())
	e := newExactTestSpillEngine(t, SpillEngineConfig{Budget: g, BuildKeyExprs: makeTestKeyExpr(), SpillThreshold: 1, MaxQueue: 1, NeedsBuildForEmptyProbe: true})
	t.Cleanup(func() { e.Cleanup(proc) })
	e.InitFromSpilledFiles([]*message.SpillFile{newTestSpillFile(writeBuildFile(proc, t.Name(), build), int64(len(values)))})
	fault.blocked = true
	_, _, err := e.RebuildHashmap(proc, process.NewAnalyzer(0, false, false, "test"))
	require.ErrorContains(t, err, "spill queue limit exceeded")
	require.ErrorIs(t, err, joinUnlinkFailure)
	require.Positive(t, g.SpillDiskUsed())
	require.Zero(t, g.SpillFDUsed())
	attempts := fault.attempts
	e.Cleanup(proc)
	require.Equal(t, attempts, fault.attempts)
	_, err = view.CreateFile(context.Background(), "new")
	require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
}
