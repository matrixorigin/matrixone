// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

type scanFailureFS struct {
	fileservice.FileService
	readPath  string
	failWrite bool
	failure   error
}

func (f *scanFailureFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if v.FilePath == f.readPath {
		return f.failure
	}
	return f.FileService.Read(ctx, v)
}
func (f *scanFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if f.failWrite && strings.HasPrefix(v.FilePath, "gc/") {
		return f.failure
	}
	return f.FileService.Write(ctx, v)
}

func TestGCWindowPublishesScanProof(t *testing.T) {
	ctx := t.Context()
	mp := common.DebugAllocator
	fs, err := fileservice.NewMemoryFS("scan-proof", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	cat := catalog.MockCatalog(nil)
	defer cat.Close()
	db, err := cat.CreateDBEntry("scan", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsLocation(stats, objectio.BuildLocation(objectio.BuildObjectNameWithObjectID(&id), objectio.NewExtent(0, 0, 1, 1), 1, 0)))
	_, err = table.CreateCommittedObject(types.BuildTS(5, 0), &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)
	start, end := types.BuildTS(1, 0), types.BuildTS(20, 0)
	data, err := logtail.IncrementalCheckpointDataFactory(start, end, 0, fs)(cat)
	require.NoError(t, err)
	defer data.Close()
	loc, _, err := data.Sync(ctx, fs)
	require.NoError(t, err)
	entry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.ET_Incremental)
	reader, err := logtail.GetCheckpointReader(ctx, "", fs, loc, logtail.CheckpointCurrentVersion)
	require.NoError(t, err)
	require.NotEmpty(t, reader.GetLocations())
	failure := errors.New("scan failed")
	for _, mode := range []string{"read error", "metadata write error", "cancel", "success"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			wrapped := &scanFailureFS{FileService: fs, failure: failure}
			if mode == "read error" {
				wrapped.readPath = reader.GetLocations()[0].Name().String()
			}
			wrapped.failWrite = mode == "metadata write error"
			window := NewGCWindow(mp, wrapped)
			defer window.Close()
			buffer := MakeGCWindowBuffer(mpool.MB)
			defer buffer.Close(mp)
			name, err := window.ScanCheckpoints(ctx, []*checkpoint.CheckpointEntry{entry}, func(ctx context.Context, _ *checkpoint.CheckpointEntry) (*logtail.CKPReader, error) {
				return logtail.GetCheckpointReader(ctx, "", wrapped, loc, logtail.CheckpointCurrentVersion)
			}, nil, func() error {
				if mode == "cancel" {
					cancel()
				}
				return nil
			}, buffer)
			if mode != "success" {
				require.Error(t, err)
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, failure)
				}
				_, proofEnd := window.ScannedRange()
				require.True(t, proofEnd.IsEmpty())
				metas, err := ioutil.ListTSRangeFilesInGCDir(t.Context(), fs)
				require.NoError(t, err)
				require.Empty(t, metas)
				return
			}
			require.NoError(t, err)
			reloaded := NewGCWindow(mp, fs)
			defer reloaded.Close()
			require.NoError(t, reloaded.ReadTable(ctx, ioutil.MakeGCFullName(name), fs))
			proofStart, proofEnd := reloaded.ScannedRange()
			require.Equal(t, start, proofStart)
			require.Equal(t, end, proofEnd)
			require.NotEmpty(t, reloaded.GetObjectStats())
			// Legacy readers load only block zero, which remains a stats-only batch.
			oldReader, err := ioutil.NewFileReaderNoCache(fs, ioutil.MakeGCFullName(name))
			require.NoError(t, err)
			blocks, err := oldReader.LoadAllBlocks(ctx, mp)
			require.NoError(t, err)
			require.Len(t, blocks, 2)
			oldData, release, err := oldReader.LoadColumns(ctx, []uint16{0}, nil, blocks[0].GetID(), mp)
			require.NoError(t, err)
			defer release()
			require.Equal(t, len(reloaded.GetObjectStats()), oldData.Vecs[0].Length())
			require.Equal(t, reloaded.GetObjectStats()[0][:], oldData.Vecs[0].GetBytesAt(0))
		})
	}
	for _, tc := range []struct {
		name                       string
		entries                    []*checkpoint.CheckpointEntry
		expectedStart, expectedEnd types.TS
		invalid                    bool
	}{
		{"global is filtered", []*checkpoint.CheckpointEntry{checkpoint.NewCheckpointEntry("", types.TS{}, types.BuildTS(21, 0), checkpoint.ET_Global)}, types.TS{}, types.TS{}, false},
		{"compacted is filtered", []*checkpoint.CheckpointEntry{checkpoint.NewCheckpointEntry("", start, types.BuildTS(22, 0), checkpoint.ET_Compacted)}, types.TS{}, types.TS{}, false},
		{"gap keeps suffix", []*checkpoint.CheckpointEntry{checkpoint.NewCheckpointEntry("", start, types.BuildTS(10, 0), checkpoint.ET_Incremental), checkpoint.NewCheckpointEntry("", types.BuildTS(15, 0), types.BuildTS(23, 0), checkpoint.ET_Incremental)}, types.BuildTS(15, 0), types.BuildTS(23, 0), false},
		{"filtered checkpoint cannot bridge gap", []*checkpoint.CheckpointEntry{checkpoint.NewCheckpointEntry("", start, types.BuildTS(10, 0), checkpoint.ET_Incremental), checkpoint.NewCheckpointEntry("", types.TS{}, types.BuildTS(20, 0), checkpoint.ET_Global), checkpoint.NewCheckpointEntry("", types.BuildTS(20, 0), types.BuildTS(24, 0), checkpoint.ET_Incremental)}, types.BuildTS(20, 0), types.BuildTS(24, 0), false},
		{"out of order", []*checkpoint.CheckpointEntry{checkpoint.NewCheckpointEntry("", types.BuildTS(15, 0), types.BuildTS(24, 0), checkpoint.ET_Incremental), checkpoint.NewCheckpointEntry("", start, types.BuildTS(10, 0), checkpoint.ET_Incremental)}, types.TS{}, types.TS{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := NewGCWindow(mp, fs)
			defer window.Close()
			buffer := MakeGCWindowBuffer(mpool.MB)
			defer buffer.Close(mp)
			name, err := window.ScanCheckpoints(ctx, tc.entries, func(ctx context.Context, _ *checkpoint.CheckpointEntry) (*logtail.CKPReader, error) {
				return logtail.GetCheckpointReader(ctx, "", fs, loc, logtail.CheckpointCurrentVersion)
			}, nil, nil, buffer)
			if tc.invalid {
				require.Error(t, err)
				require.Empty(t, name)
				return
			}
			require.NoError(t, err)
			loaded := NewGCWindow(mp, fs)
			defer loaded.Close()
			require.NoError(t, loaded.ReadTable(ctx, ioutil.MakeGCFullName(name), fs))
			a, b := loaded.ScannedRange()
			require.Equal(t, tc.expectedStart, a)
			require.Equal(t, tc.expectedEnd, b)
		})
	}

}

func TestGCWindowScanProofMergeAndEncoding(t *testing.T) {
	ctx := t.Context()
	mp := common.DebugAllocator
	fs, err := fileservice.NewMemoryFS("proof-encoding", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	ts := func(n int64) types.TS { return types.BuildTS(n, 0) }
	proven := func(a, b int64) *GCWindow {
		w := NewGCWindow(mp, fs)
		w.tsRange.start, w.tsRange.end = ts(a), ts(b)
		w.scanRange = gcScanRange{ts(a), ts(b)}
		return w
	}
	window := proven(1, 20)
	unknown := NewGCWindow(mp, fs)
	unknown.tsRange.start, unknown.tsRange.end = ts(20), ts(40)
	window.Merge(unknown)
	a, b := window.ScannedRange()
	require.Equal(t, ts(1), a)
	require.Equal(t, ts(20), b)
	adjacent := proven(20, 30)
	adjacent.scanRange.start = adjacent.scanRange.start.Next()
	window.Merge(adjacent)
	a, b = window.ScannedRange()
	require.Equal(t, ts(1), a)
	require.Equal(t, ts(30), b)
	window.Merge(proven(40, 50)) // A hole must not acquire evidence through Merge.
	a, b = window.ScannedRange()
	require.Equal(t, ts(40), a)
	require.Equal(t, ts(50), b)
	window.Merge(proven(45, 60))
	window.Merge(proven(30, 40))
	a, b = window.ScannedRange()
	require.Equal(t, ts(30), a)
	require.Equal(t, ts(60), b)
	clone := window.Clone()
	require.Equal(t, window.scanRange, clone.scanRange)
	// Empty remaining-object census still carries its proof through disk replay.
	name, err := window.writeMetaForRemainings(ctx, nil)
	require.NoError(t, err)
	read := NewGCWindow(mp, fs)
	require.NoError(t, read.ReadTable(ctx, ioutil.MakeGCFullName(name), fs))
	require.Equal(t, window.scanRange, read.scanRange)
	require.Empty(t, read.GetObjectStats())
	encode := func(start, end types.TS) []byte {
		raw := append([]byte("GCS1"), start[:]...)
		return append(raw, end[:]...)
	}
	for _, test := range []struct {
		name      string
		raw       []byte
		wantError bool
	}{
		{"legacy", nil, false}, {"truncated", []byte("GCS1"), true}, {"unknown version", make([]byte, 28), true},
		{"empty end", encode(ts(1), types.TS{}), true},
		{"reversed range", encode(ts(20), ts(10)), true},
		{"beyond recovery watermark", encode(ts(1), ts(80)), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := "gc/" + ioutil.EncodeGCMetadataName(ts(1), ts(70))
			require.NoError(t, fs.Delete(ctx, path))
			writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, path, fs)
			require.NoError(t, err)
			data := batch.NewWithSchema(false, ObjectTableMetaAttrs, ObjectTableMetaTypes)
			defer data.Clean(mp)
			_, err = writer.WriteWithoutSeqnum(data)
			require.NoError(t, err)
			if test.raw != nil {
				require.NoError(t, vector.AppendBytes(data.Vecs[0], test.raw, false, mp))
				_, err = writer.WriteWithoutSeqnum(data)
				require.NoError(t, err)
			}
			_, err = writer.WriteEnd(ctx)
			require.NoError(t, err)
			loaded := NewGCWindow(mp, fs)
			defer loaded.Close()
			err = loaded.ReadTable(ctx, path, fs)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				_, end := loaded.ScannedRange()
				require.True(t, end.IsEmpty())
			}
		})
	}
}
