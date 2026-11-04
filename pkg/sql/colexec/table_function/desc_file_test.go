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

package table_function

import (
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDescFile(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.pdf"), []byte("12345"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.txt"), []byte("12"), 0600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0700))
	proc.GetStageCache().Set("docs", stage.StageDef{Id: 1, Name: "docs", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}})

	for _, tc := range []struct {
		url  string
		want []fileservice.DirEntry
	}{
		{"stage://docs/a.pdf", []fileservice.DirEntry{{Name: path.Join(dir, "a.pdf"), Size: 5}}},
		{"stage://docs/", []fileservice.DirEntry{{Name: path.Join(dir, "a.pdf"), Size: 5}, {Name: path.Join(dir, "b.txt"), Size: 2}, {Name: path.Join(dir, "sub"), IsDir: true}}},
		{"stage://docs/*.txt", []fileservice.DirEntry{{Name: path.Join(dir, "b.txt"), Size: 2}}},
		{"stage://docs/missing", nil},
	} {
		entries, err := descFileEntries(proc, tc.url)
		require.NoError(t, err, tc.url)
		for i := range entries {
			if entries[i].IsDir {
				entries[i].Size = 0
			}
		}
		require.ElementsMatch(t, tc.want, entries, tc.url)
	}

	tf := &TableFunction{Attrs: []string{"is_dir", "file", "size"}}
	tf.ctr.retSchema = []types.Type{types.T_bool.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()}
	arg, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("stage://docs/a.pdf"), 1, proc.Mp())
	require.NoError(t, err)
	defer arg.Free(proc.Mp())
	tf.ctr.argVecs = []*vector.Vector{arg}
	s := &descFileState{}
	require.NoError(t, s.start(tf, proc, 0, nil))
	defer s.free(tf, proc, false, nil)
	require.Equal(t, 1, s.batch.RowCount())
	require.False(t, vector.GetFixedAtNoTypeCheck[bool](s.batch.Vecs[0], 0))
	require.Equal(t, path.Join(dir, "a.pdf"), s.batch.Vecs[1].GetStringAt(0))
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](s.batch.Vecs[2], 0))

	tf.Attrs = []string{"owner"}
	tf.ctr.retSchema = []types.Type{types.T_varchar.ToType()}
	s2 := &descFileState{}
	require.ErrorContains(t, s2.start(tf, proc, 0, nil), "owner is not supported by desc_file()")
	s2.free(tf, proc, false, nil)
}
