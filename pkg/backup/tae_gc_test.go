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

package backup

import (
	"context"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/stretchr/testify/require"
)

func TestCopyGCDirInheritsLastCopiedMetadataWatermark(t *testing.T) {
	ctx := context.Background()
	src := newBackupMemoryFS(t, "gc-src")
	dst := newBackupMemoryFS(t, "gc-dst")

	missing := newBackupObjectStats(t, types.Uuid{1})
	present := newBackupObjectStats(t, types.Uuid{2})
	presentData := []byte("present object")
	require.NoError(t, writeFile(ctx, src, present.ObjectName().String(), presentData))

	writeGCMetadata(t, ctx, src, types.BuildTS(1, 0), types.BuildTS(10, 0), *missing)
	lastName := writeGCMetadata(t, ctx, src, types.BuildTS(11, 0), types.BuildTS(20, 0), *present)

	files, err := CopyGCDir(ctx, src, dst, "gc", types.TS{}, types.BuildTS(30, 0))
	require.NoError(t, err)

	inheritedName := ioutil.InheritGCMetadataName(
		lastName,
		tsPtr(types.BuildTS(20, 0)),
		tsPtr(types.BuildTS(30, 0)),
	)
	require.Contains(t, taeFilePaths(files), "gc/"+inheritedName)
	require.NotContains(t, taeFilePaths(files), "gc/"+lastName)
}

func TestCopyGCDirRecordsCopiedObjectSizeInManifests(t *testing.T) {
	ctx := context.Background()
	src := newBackupMemoryFS(t, "gc-src")
	dst := newBackupMemoryFS(t, "gc-dst")

	object := newBackupObjectStats(t, types.Uuid{3})
	objectData := make([]byte, 257)
	require.NoError(t, writeFile(ctx, src, object.ObjectName().String(), objectData))
	metaName := writeGCMetadata(t, ctx, src, types.BuildTS(1, 0), types.BuildTS(10, 0), *object)

	files, err := CopyGCDir(ctx, src, dst, "gc", types.TS{}, types.TS{})
	require.NoError(t, err)
	require.Equal(t, int64(len(objectData)), taeFileSize(t, files, object.ObjectName().String()))

	require.NoError(t, saveTaeFilesList(ctx, dst, files, "2026-07-21 00:00:00", "0-0", "full"))
	listData, err := readFile(ctx, dst, taeList)
	require.NoError(t, err)
	listRows, err := fromCsvBytes(listData)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(len(objectData)), csvFieldForPath(t, listRows, object.ObjectName().String(), 1))

	metaEntry, err := dst.StatFile(ctx, "gc/"+metaName)
	require.NoError(t, err)
	sumData, err := readFile(ctx, dst, taeSum)
	require.NoError(t, err)
	sumRows, err := fromCsvBytes(sumData)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(int64(len(objectData))+metaEntry.Size, 10), sumRows[0][1])
}

func newBackupMemoryFS(t *testing.T, name string) fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(name, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	return fs
}

func newBackupObjectStats(t *testing.T, id types.Uuid) *objectio.ObjectStats {
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectName(&id, 0)))
	return stats
}

func writeGCMetadata(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	start, end types.TS,
	stats ...objectio.ObjectStats,
) string {
	bat := batch.NewWithSchema(false, gc.ObjectTableMetaAttrs, gc.ObjectTableMetaTypes)
	defer bat.Clean(common.DebugAllocator)
	for _, stat := range stats {
		require.NoError(t, vector.AppendBytes(bat.GetVector(0), stat[:], false, common.DebugAllocator))
	}

	name := ioutil.EncodeGCMetadataName(start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, ioutil.MakeFullName("gc/", name), fs)
	require.NoError(t, err)
	_, err = writer.WriteWithoutSeqnum(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)
	return name
}

func tsPtr(value types.TS) *types.TS {
	return &value
}

func taeFilePaths(files []*taeFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.path)
	}
	return paths
}

func taeFileSize(t *testing.T, files []*taeFile, path string) int64 {
	for _, file := range files {
		if file.path == path {
			return file.size
		}
	}
	t.Fatalf("tae file %q not found", path)
	return 0
}

func csvFieldForPath(t *testing.T, rows [][]string, path string, field int) string {
	for _, row := range rows {
		if len(row) > field && row[0] == path {
			return row[field]
		}
	}
	t.Fatalf("CSV row for %q not found", path)
	return ""
}
