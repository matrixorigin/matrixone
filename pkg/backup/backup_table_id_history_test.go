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

package backup

import (
	"context"
	"errors"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

func TestCopyBackupTableIDHistory(t *testing.T) {
	for _, tc := range []struct {
		name          string
		entryType     checkpoint.EntryType
		previousTick  bool
		gap           bool
		emptyIndex    bool
		missingSource bool
		dstContent    string
		wantError     string
	}{
		{name: "incremental exact predecessor", entryType: checkpoint.ET_Incremental},
		{name: "global-only exact predecessor", entryType: checkpoint.ET_Global},
		{name: "backup exact predecessor", entryType: checkpoint.ET_Backup},
		{name: "legacy global previous tick stays partial", entryType: checkpoint.ET_Global, previousTick: true, emptyIndex: true},
		{name: "indexed global previous tick cannot bridge a gap", entryType: checkpoint.ET_Global, previousTick: true, wantError: "no exact predecessor"},
		{name: "nonadjacent incremental cannot bridge a gap", entryType: checkpoint.ET_Incremental, gap: true, wantError: "no exact predecessor"},
		{name: "nonadjacent global cannot bridge a gap", entryType: checkpoint.ET_Global, gap: true, wantError: "no exact predecessor"},
		{name: "legacy incremental does not invent history", entryType: checkpoint.ET_Incremental, emptyIndex: true},
		{name: "legacy global does not invent history", entryType: checkpoint.ET_Global, emptyIndex: true},
		{name: "missing source index fails backup", entryType: checkpoint.ET_Incremental, missingSource: true, wantError: "not found"},
		{name: "existing identical index is reusable", entryType: checkpoint.ET_Incremental, dstContent: "source index bytes"},
		{name: "existing different index is not overwritten", entryType: checkpoint.ET_Incremental, dstContent: "different index bytes", wantError: "checksum mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			src := newBackupMemoryFS(t, "history-src")
			defer src.Close(context.Background())
			dst := newBackupMemoryFS(t, "history-dst")
			defer dst.Close(context.Background())
			start := types.BuildTS(100, 0)
			predecessorEnd := start.Prev()
			if tc.entryType == checkpoint.ET_Global && !tc.previousTick {
				predecessorEnd = start
			}
			if tc.gap {
				predecessorEnd = types.BuildTS(90, 0)
			}
			objectID := objectio.NewObjectid()
			name := objectio.BuildObjectNameWithObjectID(&objectID)
			location := objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)
			var locations objectio.LocationSlice
			if !tc.emptyIndex {
				locations = append(locations, location...)
				if !tc.missingSource {
					require.NoError(t, writeFile(ctx, src, name.String(), []byte("source index bytes")))
				}
				if tc.dstContent != "" {
					require.NoError(t, writeFile(ctx, dst, name.String(), []byte(tc.dstContent)))
				}
			}

			// This component owns byte copying and exact-boundary selection, not
			// index interpretation. Real history ranges are covered by the restore
			// regression; only the durable metadata format is needed here.
			func() {
				meta := containers.NewBatch()
				defer meta.Close()
				values := []any{
					types.TS{}, predecessorEnd, []byte(nil), tc.entryType != checkpoint.ET_Global,
					uint32(logtail.CheckpointCurrentVersion), []byte(nil), uint64(8), uint64(8),
					int8(tc.entryType), []byte(locations),
				}
				for col, attr := range checkpoint.CheckpointSchema.Attrs() {
					vec := containers.MakeVector(checkpoint.CheckpointSchema.Types()[col], common.CheckpointAllocator)
					meta.AddVector(attr, vec)
					vec.Append(values[col], false)
				}
				metaName := path.Clean(ioutil.EncodeCKPMetadataFullName(types.TS{}, predecessorEnd))
				writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, metaName, dst)
				require.NoError(t, err)
				_, err = writer.Write(containers.ToCNBatch(meta))
				require.NoError(t, err)
				_, err = writer.WriteEnd(ctx)
				require.NoError(t, err)
			}()

			gotLocations, files, err := copyBackupTableIDHistory(ctx, "", src, dst, start)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				if tc.missingSource {
					require.ErrorContains(t, err, name.String())
				}
				require.Empty(t, gotLocations)
				require.Empty(t, files)
				if tc.dstContent != "" {
					content, err := readFile(ctx, dst, name.String())
					require.NoError(t, err)
					require.Equal(t, tc.dstContent, string(content))
				}
				return
			}
			require.NoError(t, err)
			if tc.emptyIndex {
				require.Empty(t, gotLocations)
				require.Empty(t, files)
				return
			}
			require.Equal(t, locations, gotLocations)
			require.Len(t, files, 1)
			require.Equal(t, name.String(), files[0].path)
			checksum, err := checksumFile(ctx, src, name.String())
			require.NoError(t, err)
			require.Equal(t, checksum, files[0].checksum)
			content, err := readFile(ctx, dst, name.String())
			require.NoError(t, err)
			require.Equal(t, "source index bytes", string(content))
		})
	}
}

type backupHistoryFaultFS struct {
	fileservice.FileService
	failPath  string
	statErr   error
	readErr   error
	deleteErr error
	deleted   []string
}

func (fs *backupHistoryFaultFS) StatFile(ctx context.Context, name string) (*fileservice.DirEntry, error) {
	if name == fs.failPath && fs.statErr != nil {
		return nil, fs.statErr
	}
	return fs.FileService.StatFile(ctx, name)
}

func (fs *backupHistoryFaultFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if vector.FilePath == fs.failPath && fs.readErr != nil {
		return fs.readErr
	}
	return fs.FileService.Read(ctx, vector)
}

func (fs *backupHistoryFaultFS) Delete(ctx context.Context, names ...string) error {
	fs.deleted = append(fs.deleted, names...)
	if fs.deleteErr != nil {
		return fs.deleteErr
	}
	return fs.FileService.Delete(ctx, names...)
}

func TestCollectBackupTableIDFilesCleansOnlyUnpublishedIndexes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		failRead    bool
		failCleanup bool
	}{
		{name: "stat failure"},
		{name: "read failure", failRead: true},
		{name: "cleanup preserves original error", failCleanup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			base := newBackupMemoryFS(t, "unpublished-history")
			defer base.Close(context.Background())
			var locations objectio.LocationSlice
			var names []string
			for range 2 {
				objectID := objectio.NewObjectid()
				name := objectio.BuildObjectNameWithObjectID(&objectID)
				location := objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)
				locations = append(locations, location...)
				names = append(names, name.String())
				require.NoError(t, writeFile(ctx, base, name.String(), []byte("new index")))
			}
			// A repeated location must not broaden cleanup or duplicate a delete.
			locations = append(locations, locations[:objectio.LocationLen]...)
			require.NoError(t, writeFile(ctx, base, "retained-predecessor", []byte("old index")))
			injected := errors.New("injected index metadata failure")
			fs := &backupHistoryFaultFS{FileService: base, failPath: names[1]}
			if tc.failRead {
				fs.readErr = injected
			} else {
				fs.statErr = injected
			}
			cleanupErr := errors.New("injected index cleanup failure")
			if tc.failCleanup {
				fs.deleteErr = cleanupErr
			}
			files, err := collectBackupTableIDFiles(ctx, fs, locations, types.BuildTS(100, 0))
			require.ErrorIs(t, err, injected)
			require.Empty(t, files)
			require.Equal(t, names, fs.deleted)
			for _, name := range names {
				_, statErr := base.StatFile(ctx, name)
				if tc.failCleanup {
					require.NoError(t, statErr)
					require.ErrorIs(t, err, cleanupErr)
				} else {
					require.True(t, moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound))
				}
			}
			retained, err := readFile(ctx, base, "retained-predecessor")
			require.NoError(t, err)
			require.Equal(t, "old index", string(retained))
		})
	}
}
