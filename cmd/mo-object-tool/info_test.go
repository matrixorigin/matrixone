// Copyright 2021 Matrix Origin
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

package object

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/stretchr/testify/require"
)

func TestShowInfoLocalObject(t *testing.T) {
	dir := t.TempDir()
	filename := objectio.BuildObjectName(&types.Uuid{1}, 1).String()
	writeInfoTestObject(t, dir, filename)

	output := captureStdout(t, func() {
		require.NoError(t, showInfo(filepath.Join(dir, filename), toolfs.StorageOptions{}, objectio.OfflineKindLocal))
	})

	require.Contains(t, output, "Object: "+filepath.Join(dir, filename))
	require.Contains(t, output, "Blocks: 1")
	require.Contains(t, output, "Rows:   2")
	require.Contains(t, output, "Cols:   2")
	require.Contains(t, output, "INT")
	require.Contains(t, output, "VARCHAR")
}

func TestShowInfoWrapsOpenError(t *testing.T) {
	err := showInfo(filepath.Join(t.TempDir(), "missing"), toolfs.StorageOptions{}, objectio.OfflineKindLocal)
	require.ErrorContains(t, err, "failed to open object")
}

func TestShowInfoRemoteObjectFromConfig(t *testing.T) {
	dir := t.TempDir()
	filename := objectio.BuildObjectName(&types.Uuid{2}, 1).String()
	writeInfoTestObject(t, dir, filename)
	cfgPath := filepath.Join(t.TempDir(), "tn.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`
[[fileservice]]
backend = "DISK"
data-dir = "`+dir+`"
name = "SHARED"
`), 0644))

	output := captureStdout(t, func() {
		require.NoError(t, showInfo(filename, toolfs.StorageOptions{FSConfig: cfgPath}, objectio.OfflineKindLocal))
	})

	require.Contains(t, output, "Object: "+filename)
	require.Contains(t, output, "Blocks: 1")
	require.Contains(t, output, "Rows:   2")
}

func writeInfoTestObject(t *testing.T, dir string, filename string) {
	t.Helper()
	ctx := context.Background()
	mp := mpool.MustNewZero()
	service, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	defer service.Close(ctx)

	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterNormal, filename, service)
	require.NoError(t, err)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(10), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("ten"), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(20), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("twenty"), false, mp))
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	_, err = writer.Write(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx, objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  time.Unix(100, 0),
	})
	require.NoError(t, err)
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()
	require.NoError(t, writer.Close())
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return buf.String()
}
