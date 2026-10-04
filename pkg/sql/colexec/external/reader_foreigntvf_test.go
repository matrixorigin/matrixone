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

package external

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func foreignTVFTestCols() []*plan.ColDef {
	mk := func(name string, t types.T) *plan.ColDef {
		typ := t.ToType()
		return &plan.ColDef{
			Name: name,
			Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
		}
	}
	return []*plan.ColDef{
		mk("id", types.T_int64),
		mk("name", types.T_varchar),
		mk("score", types.T_int64),
	}
}

func newForeignTVFBatch(param *ExternalParam) *batch.Batch {
	bat := batch.NewWithSize(len(param.Cols))
	for i, col := range param.Cols {
		bat.Vecs[i] = vector.NewVec(types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale))
		bat.Attrs = append(bat.Attrs, col.Name)
	}
	return bat
}

// TestForeignTVFReaderSQLDialect drives the sql_tvf dialect end to end:
// MySQL-style escaping, \N NULLs, and typed coercion into a batch.
func TestForeignTVFReaderSQLDialect(t *testing.T) {
	proc := testutil.NewProcess(t)
	cols := foreignTVFTestCols()
	names := []string{"id", "name", "score"}
	param := BuildForeignTVFExternParam(proc, cols, names, ForeignTVFSourceSQL)

	csv := "\"1\",\"alice\",\"90\"\n" +
		"\"2\",\"bo\\nb\",\\N\n" +
		"\"3\",\"d,e\",\"75\"\n"
	r, err := NewForeignTVFReader(param, io.NopCloser(strings.NewReader(csv)))
	require.NoError(t, err)
	defer r.Close()

	bat := newForeignTVFBatch(param)
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 3, bat.RowCount())

	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
	require.Equal(t, []int64{1, 2, 3}, ids[:3])
	require.Equal(t, "bo\nb", bat.Vecs[1].GetStringAt(1))
	require.Equal(t, "d,e", bat.Vecs[1].GetStringAt(2))
	require.True(t, bat.Vecs[2].GetNulls().Contains(1))
	require.False(t, bat.Vecs[2].GetNulls().Contains(0))
}

// TestForeignTVFReaderESQLDialect drives the esql_tvf dialect: RFC 4180 with a
// header line to skip, doubled quotes, and empty numeric fields as NULL.
func TestForeignTVFReaderESQLDialect(t *testing.T) {
	proc := testutil.NewProcess(t)
	cols := foreignTVFTestCols()
	names := []string{"id", "name", "score"}
	param := BuildForeignTVFExternParam(proc, cols, names, ForeignTVFSourceESQL)
	require.Equal(t, uint64(1), param.Extern.Tail.IgnoredLines)

	csv := "id,name,score\r\n" +
		"1,alice,90\r\n" +
		"2,\"say \"\"hi\"\"\",\r\n" + // doubled quote; empty numeric -> NULL
		"3,\"d,e\",75\r\n"
	r, err := NewForeignTVFReader(param, io.NopCloser(strings.NewReader(csv)))
	require.NoError(t, err)
	defer r.Close()

	bat := newForeignTVFBatch(param)
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 3, bat.RowCount())

	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
	require.Equal(t, []int64{1, 2, 3}, ids[:3])
	require.Equal(t, `say "hi"`, bat.Vecs[1].GetStringAt(1))
	require.Equal(t, "d,e", bat.Vecs[1].GetStringAt(2))
	require.True(t, bat.Vecs[2].GetNulls().Contains(1))
}

// TestForeignTVFReaderPrunedColumns proves that output columns are matched to
// CSV fields by declared-schema name, so pruning/reordering cannot misalign.
func TestForeignTVFReaderPrunedColumns(t *testing.T) {
	proc := testutil.NewProcess(t)
	all := foreignTVFTestCols()
	names := []string{"id", "name", "score"}
	// The operator only outputs (id, score): name was pruned.
	pruned := []*plan.ColDef{all[0], all[2]}
	param := BuildForeignTVFExternParam(proc, pruned, names, ForeignTVFSourceSQL)
	require.Equal(t, int32(0), param.Attrs[0].ColFieldIndex)
	require.Equal(t, int32(2), param.Attrs[1].ColFieldIndex)

	csv := "\"1\",\"alice\",\"90\"\n\"2\",\"bob\",\\N\n"
	r, err := NewForeignTVFReader(param, io.NopCloser(strings.NewReader(csv)))
	require.NoError(t, err)
	defer r.Close()

	bat := newForeignTVFBatch(param)
	defer bat.Clean(proc.Mp())
	finished, err := r.ReadBatch(context.Background(), bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
	require.Equal(t, []int64{1, 2}, ids[:2])
	scores := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1])
	require.Equal(t, int64(90), scores[0])
	require.True(t, bat.Vecs[1].GetNulls().Contains(1))
}

// TestForeignTVFReaderBadValue proves a malformed value errors instead of being
// silently coerced.
func TestForeignTVFReaderBadValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	cols := foreignTVFTestCols()
	param := BuildForeignTVFExternParam(proc, cols, []string{"id", "name", "score"}, ForeignTVFSourceSQL)
	csv := "\"notanint\",\"alice\",\"90\"\n"
	r, err := NewForeignTVFReader(param, io.NopCloser(strings.NewReader(csv)))
	require.NoError(t, err)
	defer r.Close()
	bat := newForeignTVFBatch(param)
	defer bat.Clean(proc.Mp())
	_, err = r.ReadBatch(context.Background(), bat, proc, nil)
	require.Error(t, err)
}

// TestForeignTVFRawReader covers the schema-less mode: raw string fields with
// the header skipped for the ESQL dialect.
func TestForeignTVFRawReader(t *testing.T) {
	proc := testutil.NewProcess(t)
	param := BuildForeignTVFExternParam(proc, nil, nil, ForeignTVFSourceESQL)

	csv := "h1,h2\r\na,1\r\n\"d,e\",2\r\n"
	r, err := NewForeignTVFRawReader(param, io.NopCloser(strings.NewReader(csv)))
	require.NoError(t, err)
	defer r.Close()

	row, ok, err := r.ReadRow()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"a", "1"}, row)

	row, ok, err = r.ReadRow()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"d,e", "2"}, row)

	_, ok, err = r.ReadRow()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, r.Close())
	require.NoError(t, r.Close()) // idempotent
}
