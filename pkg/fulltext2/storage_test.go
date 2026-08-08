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

package fulltext2

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func testStorageCfg() TableConfig {
	return TableConfig{DbName: "db", SrcTable: "src", IndexTable: "__store", MetadataTable: "__meta"}
}

func TestSubIndexId(t *testing.T) {
	require.Equal(t, "build:7", SubIndexId("build", 7))
	require.Equal(t, "a:b:c:0", SubIndexId("a:b:c", 0))
}

func TestDeleteSqls(t *testing.T) {
	cfg := testStorageCfg()

	del := DeleteSqls(cfg, "idx:1")
	require.Len(t, del, 2)
	require.Contains(t, del[0], "__store")
	require.Contains(t, del[0], "idx:1")
	require.Contains(t, del[1], "__meta")

	all := DeleteAllBasesSqls(cfg)
	require.Len(t, all, 2)
	require.Contains(t, all[0], "__store")
	require.Contains(t, all[1], "__meta")

	tail := DeleteTailSqls(cfg)
	require.Len(t, tail, 1)
	require.Contains(t, tail[0], "__store")
}

func TestFileChunkInsertSqls(t *testing.T) {
	cfg := testStorageCfg()

	// a small file fits in ONE chunk row and ONE INSERT.
	one := fileChunkInsertSqls(cfg, "idx:0", 0, "/tmp/spill", 10, int(vectorindex.Tag_ModelChunk))
	require.Len(t, one, 1)
	require.Contains(t, one[0], "load_file(")
	require.Contains(t, one[0], "offset=0")
	require.Contains(t, one[0], "size=10")

	// a file spanning > maxInsertTuples chunks splits into multiple INSERT statements.
	// 101 chunks ⇒ one full 100-tuple INSERT + one trailing INSERT.
	dataLen := 101 * vectorindex.MaxChunkSize
	many := fileChunkInsertSqls(cfg, "idx:0", 0, "/tmp/spill", dataLen, int(vectorindex.Tag_ModelChunk))
	require.Len(t, many, 2)
	// every chunk contributes one VALUES tuple; total tuples == 101.
	require.Equal(t, 101, strings.Count(many[0], "load_file(")+strings.Count(many[1], "load_file("))

	// the tail helper delegates to fileChunkInsertSqls with CdcTailId + Tag_CdcEvents.
	tail := TailFileInsertSqls(cfg, 5, "/tmp/frame", vectorindex.MaxChunkSize+1)
	require.Len(t, tail, 1)
	require.Contains(t, tail[0], vectorindex.CdcTailId)
}

func TestNextTailChunkIdSql(t *testing.T) {
	sql := NextTailChunkIdSql(testStorageCfg())
	require.Contains(t, sql, "GREATEST")
	require.Contains(t, sql, vectorindex.CdcTailId)
	require.Contains(t, sql, "__store")
	require.Contains(t, sql, "__meta")
}

func TestFrameChunkCount(t *testing.T) {
	require.Equal(t, int64(1), FrameChunkCount(0))                            // empty ⇒ at least one
	require.Equal(t, int64(1), FrameChunkCount(1))                            // partial ⇒ one
	require.Equal(t, int64(1), FrameChunkCount(vectorindex.MaxChunkSize))     // exact ⇒ one
	require.Equal(t, int64(2), FrameChunkCount(vectorindex.MaxChunkSize+1))   // spillover ⇒ two
	require.Equal(t, int64(3), FrameChunkCount(2*vectorindex.MaxChunkSize+1)) // ceil
}

func TestToInsertSqls(t *testing.T) {
	cfg := testStorageCfg()

	b := NewBuilder("uid:0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = SubIndexId("uid", 0)
	seg.Recency = 3

	// nil sqlproc ⇒ createLocalSpillFile falls back to an os temp file.
	sqls, cleanup, err := seg.ToInsertSqls(nil, cfg, 12345, int(vectorindex.Tag_ModelChunk))
	require.NoError(t, err)
	require.NotNil(t, cleanup)
	defer cleanup()

	// first SQL is the metadata row; the rest persist the chunk bytes.
	require.GreaterOrEqual(t, len(sqls), 2)
	require.Contains(t, sqls[0], "__meta")
	require.Contains(t, sqls[0], "uid:0")
	require.Contains(t, sqls[1], "__store")
	require.Contains(t, sqls[1], "load_file(")
}

func TestCreateLocalSpillAndTempFile(t *testing.T) {
	// nil sqlproc ⇒ both spill helpers fall back to the OS temp dir.
	require.Equal(t, "", localSpillDir(nil))

	fp, err := createLocalSpillFile(nil, "ft2test")
	require.NoError(t, err)
	name := fp.Name()
	require.NoError(t, fp.Close())
	require.NoError(t, os.Remove(name))

	f, path, err := createLocalTempFile(nil, "ft2test")
	require.NoError(t, err)
	require.NotEmpty(t, path) // linked file in the OS temp dir
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove(path))
}

func TestLocalSpillDirNilRootFS(t *testing.T) {
	require.Equal(t, "", LocalSpillDir(context.Background(), nil))
}
