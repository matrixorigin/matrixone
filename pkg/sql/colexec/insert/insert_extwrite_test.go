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

package insert

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// mockExtWriter records ExternalWriter lifecycle calls.
type mockExtWriter struct {
	writes  int
	closes  int
	aborts  int
	rows    uint64
	failure error
}

func (m *mockExtWriter) WriteBatch(ctx context.Context, bat *batch.Batch) error {
	m.writes++
	if m.failure != nil {
		return m.failure
	}
	m.rows += uint64(bat.RowCount())
	return nil
}

func (m *mockExtWriter) Close(ctx context.Context) (uint64, error) {
	m.closes++
	return m.rows, m.failure
}

func (m *mockExtWriter) Abort(ctx context.Context) {
	m.aborts++
}

func notNullCol(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:    name,
		Default: &plan.Default{NullAbility: false},
	}
}

func nullableCol(name string) *plan.ColDef {
	return &plan.ColDef{
		Name:    name,
		Default: &plan.Default{NullAbility: true},
	}
}

// TestCheckExternalNotNull covers the NOT NULL matrix: nulls bitmap, const-null
// vectors, the explicit NotNull flag without a Default, and skipped entries.
func TestCheckExternalNotNull(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := mpool.MustNewZero()

	newInsert := func(cols []*plan.ColDef, attrs []string) *Insert {
		ins := NewArgument()
		ins.ToExternal = true
		ins.InsertCtx = &InsertCtx{Attrs: attrs}
		ins.ctr.extCols = cols
		return ins
	}

	intVec := func(v int64, isNull bool) *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](vec, v, isNull, mp))
		return vec
	}

	// non-null value through a NOT NULL column: ok
	ins := newInsert([]*plan.ColDef{notNullCol("a")}, []string{"a"})
	defer ins.Release()
	bat := batch.New([]string{"a"})
	bat.Vecs[0] = intVec(7, false)
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	require.NoError(t, ins.checkExternalNotNull(proc, bat))

	// NULL in the nulls bitmap: rejected
	bat2 := batch.New([]string{"a"})
	bat2.Vecs[0] = intVec(0, true)
	bat2.SetRowCount(1)
	defer bat2.Clean(mp)
	err := ins.checkExternalNotNull(proc, bat2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be null")

	// const-null vector: rejected (no bitmap entry to find)
	cn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	bat3 := batch.New([]string{"a"})
	bat3.Vecs[0] = cn
	bat3.SetRowCount(3)
	defer bat3.Clean(mp)
	require.Error(t, ins.checkExternalNotNull(proc, bat3))

	// the explicit NotNull flag alone (no Default descriptor) is honored
	flagOnly := &plan.ColDef{Name: "a", NotNull: true}
	ins2 := newInsert([]*plan.ColDef{flagOnly}, []string{"a"})
	defer ins2.Release()
	bat4 := batch.New([]string{"a"})
	bat4.Vecs[0] = intVec(0, true)
	bat4.SetRowCount(1)
	defer bat4.Clean(mp)
	require.Error(t, ins2.checkExternalNotNull(proc, bat4))

	// nullable column and nil extCols entries pass NULLs through
	ins3 := newInsert([]*plan.ColDef{nullableCol("a"), nil}, []string{"a", "b"})
	defer ins3.Release()
	bat5 := batch.New([]string{"a", "b"})
	bat5.Vecs[0] = intVec(0, true)
	bat5.Vecs[1] = intVec(0, true)
	bat5.SetRowCount(1)
	defer bat5.Clean(mp)
	require.NoError(t, ins3.checkExternalNotNull(proc, bat5))
}

// TestExternalResetAborts: a writer still alive at Reset/Free means the stream
// did not end cleanly — the file must be aborted, never finalized.
func TestExternalResetAborts(t *testing.T) {
	proc := testutil.NewProc(t)

	ins := NewArgument()
	ins.ToExternal = true
	ins.InsertCtx = &InsertCtx{}
	mock := &mockExtWriter{}
	ins.ctr.extWriter = mock

	ins.Reset(proc, true, nil)
	require.Equal(t, 1, mock.aborts)
	require.Equal(t, 0, mock.closes)
	require.Nil(t, ins.ctr.extWriter)

	// Free with a live writer aborts too.
	mock2 := &mockExtWriter{}
	ins.ctr.extWriter = mock2
	ins.Free(proc, true, nil)
	require.Equal(t, 1, mock2.aborts)
	require.Equal(t, 0, mock2.closes)

	ins.Release()
}
