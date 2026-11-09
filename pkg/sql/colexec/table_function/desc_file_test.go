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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		require.ElementsMatch(t, tc.want, entries, tc.url)
	}

	// each row of a non-constant argument is described by its own call
	rows := vector.NewVec(types.T_varchar.ToType())
	defer rows.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(rows, []byte("stage://docs/a.pdf"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(rows, []byte("stage://docs/"), false, proc.Mp()))
	tf := &TableFunction{Attrs: []string{"file", "size"}}
	tf.ctr.retSchema = []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}
	tf.ctr.argVecs = []*vector.Vector{rows}
	sr := &descFileState{}
	defer sr.free(tf, proc, false, nil)
	require.NoError(t, sr.start(tf, proc, 1, nil))
	require.Equal(t, 3, sr.batch.RowCount())
	for i := 0; i < 3; i++ {
		if sr.batch.Vecs[0].GetStringAt(i) == path.Join(dir, "sub") {
			require.Zero(t, vector.GetFixedAtNoTypeCheck[int64](sr.batch.Vecs[1], i), "a directory has size 0")
		}
	}
	tf.ctr.argVecs = nil
	require.ErrorContains(t, sr.start(tf, proc, 0, nil), "requires one argument")

	tf = &TableFunction{Attrs: []string{"is_dir", "file", "size"}}
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

	// a NULL or empty path returns no rows; a path that is not a string is an error
	null := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer null.Free(proc.Mp())
	untyped := vector.NewConstNull(types.T_any.ToType(), 1, proc.Mp())
	defer untyped.Free(proc.Mp())
	empty, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(""), 1, proc.Mp())
	require.NoError(t, err)
	defer empty.Free(proc.Mp())
	for _, v := range []*vector.Vector{null, untyped, empty} {
		tf.ctr.argVecs = []*vector.Vector{v}
		sn := &descFileState{}
		require.NoError(t, sn.start(tf, proc, 0, nil))
		require.Zero(t, sn.batch.RowCount())
		sn.free(tf, proc, false, nil)
	}
	num, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, proc.Mp())
	require.NoError(t, err)
	defer num.Free(proc.Mp())
	tf.ctr.argVecs = []*vector.Vector{num}
	sn := &descFileState{}
	require.ErrorContains(t, sn.start(tf, proc, 0, nil), "filepath must be string")
	sn.free(tf, proc, false, nil)
	tf.ctr.argVecs = []*vector.Vector{arg}

	tf.Attrs = []string{"owner"}
	tf.ctr.retSchema = []types.Type{types.T_varchar.ToType()}
	s2 := &descFileState{}
	require.ErrorContains(t, s2.start(tf, proc, 0, nil), "owner is not supported by desc_file()")
	s2.free(tf, proc, false, nil)
}

func TestTableFunctionPrepareDescFile(t *testing.T) {
	proc := testutil.NewProcess(t)
	tf := &TableFunction{
		FuncName: "desc_file",
		Attrs:    []string{"file"},
		Rets:     []*plan.ColDef{{Name: "file", Typ: plan.Type{Id: int32(types.T_varchar)}}},
	}
	require.NoError(t, tf.Prepare(proc))
	require.IsType(t, &descFileState{}, tf.ctr.state)
	tf.ctr.state.free(tf, proc, false, nil)
}

// desc_file admits a CN path only for the sys account's moadmin role, like stage_list.
func TestDescFileRequiresServerFileAuthority(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Account = "acc1"
	proc.Base.SessionInfo.Role = "accountadmin"
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.pdf"), []byte("12345"), 0600))
	proc.GetStageCache().Set("docs", stage.StageDef{Id: 1, Name: "docs", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}})
	for _, u := range []string{"stage://docs/a.pdf", "stage://docs/", "stage://docs/*.pdf", "stage://docs/missing"} {
		_, err := descFileEntries(proc, u)
		require.ErrorContains(t, err, "only the sys account moadmin role can access SQL file paths", u)
	}
}
