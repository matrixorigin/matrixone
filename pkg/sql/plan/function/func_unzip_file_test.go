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

package function

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func writeTestZip(t *testing.T, path string, files ...[2]string) {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range files {
		w, err := zw.Create(f[0])
		require.NoError(t, err)
		_, err = w.Write([]byte(f[1]))
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
}

func TestUnzipFile(t *testing.T) {
	dir := t.TempDir()
	proc := newSQLFileAccessTestProc(t)
	docs := filepath.Join(dir, "docs.zip")
	writeTestZip(t, docs, [2]string{"d/", ""}, [2]string{".hidden", "skip"}, [2]string{"a.txt", "hello zip"}, [2]string{"b.txt", "second"})
	empty := filepath.Join(dir, "empty.zip")
	writeTestZip(t, empty, [2]string{"e.txt", ""})
	plain := filepath.Join(dir, "plain.txt")
	require.NoError(t, os.WriteFile(plain, []byte("not a zip"), 0o600))

	testCases := []struct {
		info   string
		typ    types.T
		paths  []string
		con    bool
		want   []string
		nulls  []bool
		errStr string
	}{
		{info: "constant path", typ: types.T_varchar, paths: []string{docs, docs}, con: true, want: []string{"hello zip", "hello zip"}},
		{info: "row paths", typ: types.T_varchar, paths: []string{docs, empty}, want: []string{"hello zip", ""}, nulls: []bool{false, true}},
		{info: "datalink", typ: types.T_datalink, paths: []string{"file://" + docs}, want: []string{"hello zip"}},
		{info: "not a zip archive", typ: types.T_varchar, paths: []string{plain}, errStr: "not a zip archive"},
		{info: "constant NULL", typ: types.T_varchar, paths: []string{""}, con: true, want: []string{""}, nulls: []bool{true}},
		{info: "NULL row", typ: types.T_varchar, paths: []string{docs, ""}, want: []string{"hello zip", ""}, nulls: []bool{false, true}},
		{info: "missing file", typ: types.T_varchar, paths: []string{filepath.Join(dir, "missing.zip")}, errStr: "missing"},
		{info: "invalid datalink", typ: types.T_datalink, paths: []string{"ftp://x/a.zip"}, errStr: "unsupported"},
	}
	for _, tc := range testCases {
		open := openUnzipFilePath
		if tc.typ == types.T_datalink {
			open = openUnzipFileDatalink
		}
		input := NewFunctionTestInput(tc.typ.ToType(), tc.paths, tc.nulls)
		if tc.con {
			input = NewFunctionTestConstInput(tc.typ.ToType(), tc.paths, tc.nulls)
		}
		expect := NewFunctionTestResult(types.T_blob.ToType(), tc.errStr != "", tc.want, tc.nulls)
		fcTC := NewFunctionTestCase(proc, []FunctionTestInput{input}, expect, unzipFile(open))
		s, info := fcTC.RunAndFree()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// unzip_file resolves for a path of either string type and for a datalink, returning a BLOB.
func TestUnzipFileOverloads(t *testing.T) {
	for _, typ := range []types.T{types.T_varchar, types.T_char, types.T_datalink} {
		resolved, err := GetFunctionByName(context.Background(), "unzip_file", []types.Type{typ.ToType()})
		require.NoError(t, err, typ.String())
		require.Equal(t, types.T_blob, resolved.GetReturnType().Oid)
		ov, err := GetFunctionById(context.Background(), resolved.GetEncodedOverloadID())
		require.NoError(t, err)
		exec, _, _, _ := ov.GetExecuteMethod()
		require.NotNil(t, exec, typ.String())
	}
}

// unzip_file admits a CN path only for the sys account's moadmin role, like load_file.
func TestUnzipFileRequiresServerFileAuthority(t *testing.T) {
	dir := t.TempDir()
	docs := filepath.Join(dir, "docs.zip")
	writeTestZip(t, docs, [2]string{"a.txt", "hello zip"})
	proc := newSQLFileAccessTestProc(t)
	proc.Base.SessionInfo.Account = "acc1"
	proc.Base.SessionInfo.Role = "accountadmin"
	proc.Ctx = process.SQLFileAccessContext(context.Background(), proc)
	for _, tc := range []struct {
		typ  types.T
		path string
		open func(string, *process.Process) (io.ReadCloser, error)
	}{
		{types.T_varchar, docs, openUnzipFilePath},
		{types.T_datalink, "file://" + docs, openUnzipFileDatalink},
	} {
		input := NewFunctionTestInput(tc.typ.ToType(), []string{tc.path}, nil)
		expect := NewFunctionTestResult(types.T_blob.ToType(), true, []string{""}, nil)
		fcTC := NewFunctionTestCase(proc, []FunctionTestInput{input}, expect, unzipFile(tc.open))
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)
		_, err := tc.open(tc.path, proc)
		require.ErrorContains(t, err, "only the sys account moadmin role can access SQL file paths")
	}
}

// unzip_file reads at most the blob limit plus one byte of the decompressed file.
func TestUnzipFileBlobLimit(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("big.bin")
	require.NoError(t, err)
	zeros := make([]byte, 1<<20)
	for written := 0; written <= types.MaxBlobLen; written += len(zeros) {
		_, err = w.Write(zeros)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	big := filepath.Join(t.TempDir(), "big.zip")
	require.NoError(t, os.WriteFile(big, buf.Bytes(), 0o600))

	proc := newSQLFileAccessTestProc(t)
	r, err := openUnzipFilePath(big, proc)
	require.NoError(t, err)
	_, err = unzipContents(r, proc)
	require.ErrorContains(t, err, "Data too long for blob")
}

// A canceled statement fails the archive read instead of returning its contents.
func TestUnzipFileCanceled(t *testing.T) {
	docs := filepath.Join(t.TempDir(), "docs.zip")
	writeTestZip(t, docs, [2]string{"a.txt", "hello zip"})
	proc := newSQLFileAccessTestProc(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	_, err := openUnzipFilePath(docs, proc)
	require.ErrorIs(t, err, context.Canceled)
}

type cancelAfterRead struct {
	io.Reader
	cancel context.CancelFunc
	reads  int
}

func (c *cancelAfterRead) Read(p []byte) (int, error) {
	c.reads++
	c.cancel()
	return c.Reader.Read(p[:min(len(p), 512)])
}

func (c *cancelAfterRead) Close() error { return nil }

func TestUnzipFileCanceledWhileReading(t *testing.T) {
	big := make([]byte, 1<<16)
	for i, x := 0, uint32(1); i < len(big); i++ {
		x = x*1664525 + 1013904223
		big[i] = byte(x >> 24)
	}
	for _, files := range [][][2]string{
		{{"a.txt", string(big)}},
		{{".hidden", string(big)}, {"a.txt", "hello zip"}},
	} {
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		for _, f := range files {
			w, err := zw.Create(f[0])
			require.NoError(t, err)
			_, err = w.Write([]byte(f[1]))
			require.NoError(t, err)
		}
		require.NoError(t, zw.Close())

		proc := newSQLFileAccessTestProc(t)
		ctx, cancel := context.WithCancel(proc.Ctx)
		proc.Ctx = ctx
		r := &cancelAfterRead{Reader: bytes.NewReader(buf.Bytes()), cancel: cancel}
		_, err := unzipContents(r, proc)
		require.ErrorIs(t, err, context.Canceled, files[0][0])
		require.Equal(t, 1, r.reads, files[0][0])
	}
}
