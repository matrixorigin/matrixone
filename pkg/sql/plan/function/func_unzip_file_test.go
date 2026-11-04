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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	}
	for _, tc := range testCases {
		open := openUnzipFilePath
		if tc.typ == types.T_datalink {
			open = openUnzipFileDatalink
		}
		input := NewFunctionTestInput(tc.typ.ToType(), tc.paths, nil)
		if tc.con {
			input = NewFunctionTestConstInput(tc.typ.ToType(), tc.paths, nil)
		}
		expect := NewFunctionTestResult(types.T_blob.ToType(), tc.errStr != "", tc.want, tc.nulls)
		fcTC := NewFunctionTestCase(proc, []FunctionTestInput{input}, expect, unzipFile(open))
		s, info := fcTC.RunAndFree()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
