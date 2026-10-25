// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi_update

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type admissionThrottler struct {
	capacity, reserved  int64
	granted, released   int64
	acquires, refreshes int
	invalidRelease      bool
	beforeAcquire       func(*admissionThrottler)
	beforeWait          func(*admissionThrottler)
}

func (m *admissionThrottler) Refresh()         { m.refreshes++ }
func (m *admissionThrottler) ForceRefresh()    { m.refreshes++ }
func (m *admissionThrottler) PrintUsage()      {}
func (m *admissionThrottler) Available() int64 { return m.capacity - m.reserved }
func (m *admissionThrottler) Acquire(size int64) (int64, bool) {
	m.acquires++
	if m.beforeAcquire != nil {
		m.beforeAcquire(m)
	}
	if size > m.Available() {
		return m.Available(), false
	}
	m.reserved += size
	m.granted += size
	return m.Available(), true
}
func (m *admissionThrottler) MaxAcquire() int64 { return 1 << 40 }
func (m *admissionThrottler) BatchBytes() int64 { return 64 << 20 }
func (m *admissionThrottler) AcquireContext(ctx context.Context, size int64) error {
	if m.beforeWait != nil {
		m.beforeWait(m)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if _, ok := m.Acquire(size); ok {
		return nil
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return errors.New("test admission has no release notification")
}
func (m *admissionThrottler) Release(size int64) int64 {
	if size > m.reserved {
		m.invalidRelease = true
	}
	m.reserved -= size
	m.released += size
	return m.Available()
}

func TestS3WriterRejectsWideBatchBeforeCopy(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	// A constant vector models a >=64 MiB logical batch with only one scalar
	// allocated. Denial must happen before any projection, clone, or FS access.
	vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1<<23, proc.Mp())
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vec
	bat.SetRowCount(1 << 23)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	require.GreaterOrEqual(t, bat.Size(), 64<<20)
	m := &admissionThrottler{}
	ctx, cancel := context.WithCancel(proc.Ctx)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	m.beforeAcquire = func(m *admissionThrottler) {
		if m.acquires == 2 {
			cancel()
		}
	}
	w := &s3WriterDelegate{
		updateCtxs:     []*MultiUpdateCtx{{InsertCols: []int{0}}},
		insertSinkers:  make([]*colexec.CNS3Writer, 1),
		flushThreshold: 64 << 20,
	}
	w.memController.throttler = m
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	err = w.append(proc, process.NewAnalyzer(0, false, false, "admission"), bat)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, m.acquires)
	require.Zero(t, m.reserved)
	require.Nil(t, w.insertSinkers[0])
	require.Zero(t, w.batchSize)
}

type writeErrorFS struct {
	fileservice.FileService
	err error
}

func (fs *writeErrorFS) Write(context.Context, fileservice.IOVector) error { return fs.err }

func TestS3WriterMemoryPressure(t *testing.T) {
	for _, mode := range []string{"flush and retry", "wait for capacity", "flush error", "cancel before growth"} {
		t.Run(mode, func(t *testing.T) {
			_, ctrl, proc := prepareTestCtx(t, true)
			t.Cleanup(proc.Free)
			fs := proc.GetFileService()
			t.Cleanup(func() { fs.Close(context.Background()) })
			writeErr := errors.New("injected object write failure")
			if mode == "flush error" {
				proc.Base.FileService = &writeErrorFS{FileService: fs, err: writeErr}
			}
			bat := batch.NewWithSize(6)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Attrs = []string{"a", "b", "c", "d", "uk_pk", "sk_pk"}
			bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
			bat.Vecs[1] = testutil.NewStringVector(2, types.T_varchar.ToType(), proc.Mp(), false, nil, nil)
			bat.Vecs[2] = testutil.NewInt32Vector(2, types.T_int32.ToType(), proc.Mp(), false, nil, nil)
			bat.Vecs[3] = testutil.NewInt32Vector(2, types.T_int32.ToType(), proc.Mp(), false, nil, nil)
			bat.Vecs[4] = testutil.MakeVarcharVector([]string{"1", "2"}, nil, proc.Mp())
			bat.Vecs[5] = testutil.MakeVarcharVector([]string{"1", "2"}, nil, proc.Mp())
			bat.SetRowCount(2)
			tc := buildTestCase(prepareTestInsertMultiUpdateCtx(true, true), prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
			for i, c := range tc.op.MultiUpdateCtx {
				c.TableDef.TblId = uint64(100 + i)
			}
			t.Cleanup(func() { tc.op.Free(proc, false, nil) })
			require.NoError(t, tc.op.Prepare(proc))
			w := tc.op.ctr.s3Writer
			m := &admissionThrottler{capacity: 1 << 30}
			w.memController.throttler = m
			w.flushThreshold = 64 << 20
			require.NoError(t, w.append(proc, tc.op.OpAnalyzer, bat))
			require.Positive(t, m.reserved)
			require.Equal(t, w.retainedBytes(), m.reserved, "charge actual current capacity, including free-list ownership")
			first := append([]*colexec.CNS3Writer(nil), w.insertSinkers...)
			for _, sinker := range first {
				require.NotNil(t, sinker)
			}
			beforeReleased := m.released
			m.capacity = m.reserved // the next growth must flush the previous owner
			if mode == "wait for capacity" {
				m.capacity = 0
				m.beforeWait = func(m *admissionThrottler) { m.capacity = 1 << 30 }
			}
			if mode == "cancel before growth" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				defer cancel()
				proc.Ctx = ctx
				cancel()
				require.ErrorIs(t, w.append(proc, tc.op.OpAnalyzer, bat), context.Canceled)
				require.Equal(t, 1, m.acquires)
			} else if mode == "flush error" {
				require.ErrorIs(t, w.append(proc, tc.op.OpAnalyzer, bat), writeErr)
				require.Equal(t, 2, m.acquires)
				require.Equal(t, beforeReleased, m.released, "failed flush retains its private vector owner")
			} else {
				require.NoError(t, w.append(proc, tc.op.OpAnalyzer, bat))
				for i, sinker := range w.insertSinkers {
					require.NotSame(t, first[i], sinker)
				}
				require.Positive(t, w.metadataWritten, "first flush metadata is disk owned while Build continues")
				rows := map[uint64]uint64{}
				for {
					require.NoError(t, w.flushTailAndWriteToOutput(proc, tc.op.OpAnalyzer))
					if w.outputBat == nil {
						break
					}
					tables := vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[1])
					counts := vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2])
					for i, table := range tables {
						rows[table] += counts[i]
					}
				}
				for _, c := range w.updateCtxs {
					require.Equal(t, uint64(4), rows[c.TableDef.TblId], "each source row published once for main and indexes")
				}
				require.NoError(t, w.reset(proc))
				require.Zero(t, w.retainedBytes())
				require.Zero(t, m.reserved)
				m.capacity = 1 << 30
				require.NoError(t, w.append(proc, tc.op.OpAnalyzer, bat))
			}

			require.NoError(t, w.free(proc))
			require.Zero(t, m.reserved)
			require.Equal(t, m.granted, m.released)
			require.False(t, m.invalidRelease)
		})
	}
}
