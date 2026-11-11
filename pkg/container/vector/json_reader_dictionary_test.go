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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderSnapshotDictionary(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":"first"}`), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":"third","unused":[1,2]}`), false, mp))
	rows, err := NewRowSelection([]int64{2, 1, 0, 2}, 3, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	dict, err := source.SelectDictionary(rows, mp, nil)
	require.NoError(t, err)
	defer dict.Free(mp)
	checked, err := dict.GetBytesAtChecked(0)
	require.NoError(t, err)
	require.Equal(t, jsonAdmissionValue(t, `{"keep":"third","unused":[1,2]}`), checked)
	reader, err := NewJSONReaderSnapshot(dict, mp)
	require.NoError(t, err)
	defer reader.Close()
	path := *jsonReaderPath(t, "$.keep")
	for row, want := range []string{"third", "", "first", "third"} {
		selected, err := reader.QueryTextAtPath(row, path, false)
		require.NoError(t, err)
		if row == 1 {
			require.True(t, selected.InputNull)
		} else {
			require.True(t, selected.Exists)
			require.Equal(t, want, selected.Text)
		}
		selected.Close()
	}
	dict.Free(mp)
	source.Free(mp)
	selected, err := reader.QueryTextAtPath(0, path, false)
	require.NoError(t, err)
	require.Equal(t, "third", selected.Text)
	selected.Close()
}

func TestJSONReaderSnapshotDictionaryRejectsCorruption(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":1,"large":[0,0]}`), false, mp))
	rows, err := NewRowSelection([]int64{0}, 1, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	dict, err := source.SelectDictionary(rows, mp, nil)
	require.NoError(t, err)
	defer dict.Free(mp)
	base := dict.dictionaryState().base.vec
	root := types.DecodeJson(base.GetBytesAt(0))
	root.GetObjectVal(1).Data[8+5] = 0xfd
	reader, invalid, err := TryNewJSONReaderSnapshotForRows(dict, 1, mp, nil)
	require.Error(t, err)
	require.True(t, invalid)
	require.Nil(t, reader)
	root.GetObjectVal(1).Data[8+5] = bytejson.TpCodeInt64
	descriptor := &types.DecodeSlice[types.Varlena](base.data)[0]
	saved := *descriptor
	descriptor.SetOffsetLen(^uint32(0), 100)
	reader, invalid, err = TryNewJSONReaderSnapshotForRows(dict, 1, mp, nil)
	require.Error(t, err)
	require.True(t, invalid)
	require.Nil(t, reader)
	*descriptor = saved
}

func TestJSONReaderSnapshotDictionaryAllocationFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `{"keep":"value"}`), false, mp))
	rows, err := NewRowSelection([]int64{0}, 1, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	dict, err := source.SelectDictionary(rows, mp, nil)
	require.NoError(t, err)
	defer dict.Free(mp)
	const capacity = 1 << 20
	limited, err := mpool.NewMPool(t.Name(), capacity, mpool.NoLock)
	require.NoError(t, err)
	defer func() { require.Zero(t, limited.CurrNB()); mpool.DeleteMPool(limited) }()
	filler, err := limited.Alloc(capacity, true)
	require.NoError(t, err)
	defer limited.Free(filler)
	reader, invalid, err := TryNewJSONReaderSnapshotForRows(dict, 1, limited, nil)
	require.Error(t, err)
	require.False(t, invalid, "allocation failure must remain terminal")
	require.Nil(t, reader)
	require.True(t, dict.IsDictionary())
	_, err = dict.GetBytesAtChecked(0)
	require.NoError(t, err)
}
