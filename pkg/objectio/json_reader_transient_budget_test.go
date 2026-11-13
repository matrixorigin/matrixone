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

package objectio

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCacheJSONReaderTransientBackingAdmission(t *testing.T) {
	for _, operation := range []string{"select", "materialize"} {
		t.Run(operation, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			source := vector.NewVec(types.T_json.ToType())
			defer source.Free(mp)
			doc, err := types.ParseStringToByteJson(`{"keep":"` + strings.Repeat("x", 32<<10) + `"}`)
			require.NoError(t, err)
			require.NoError(t, vector.AppendByteJson(source, doc, false, mp))
			data := privateJSONColumnForTest(t, source)
			defer data.Release()
			want := errors.New("budget exhausted")
			var live uint64
			limit := uint64(1 << 30)
			admission := func(size uint64) (func(), error) {
				if live > limit || size > limit-live {
					return nil, want
				}
				live += size
				return func() { live -= size }, nil
			}
			reader, err := OpenJSONReader(data, mp, nil, admission)
			require.NoError(t, err)
			defer reader.Close()
			require.True(t, IsPrivateJSONCacheReader(reader))
			baseline := live
			limit = live + 8192
			if operation == "select" {
				selected, err := reader.Select(make([]int64, 4096), mp)
				if selected != nil {
					defer selected.Close()
				}
				require.ErrorIs(t, err, want)
				require.Nil(t, selected)
			} else {
				materialized, err := reader.Materialize(mp)
				if materialized != nil {
					defer materialized.Free(mp)
				}
				require.ErrorIs(t, err, want)
				require.Nil(t, materialized)
			}
			require.Equal(t, baseline, live)
			reader.Close()
			require.Zero(t, live)
		})
	}
}

func TestJSONReadSetTransientDeleteMappingAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	builder, err := vector.NewJSONReaderBuilder(mp, nil)
	require.NoError(t, err)
	defer builder.Close()
	doc, err := types.ParseStringToByteJson("1")
	require.NoError(t, err)
	raw, err := doc.Marshal()
	require.NoError(t, err)
	for range 4097 {
		require.NoError(t, builder.Append(raw, false))
	}
	reader, err := builder.Finish()
	require.NoError(t, err)
	defer reader.Close()
	want := errors.New("budget exhausted")
	var live uint64
	limit := uint64(1 << 30)
	admission := func(size uint64) (func(), error) {
		if live > limit || size > limit-live {
			return nil, want
		}
		live += size
		return func() { live -= size }, nil
	}
	set, err := NewJSONReadSetWithAdmission([]int{0}, nil, admission)
	require.NoError(t, err)
	defer set.Close()
	set.BindColumns([]uint16{7})
	set.Publish(7, reader)
	limit = live + 8192
	require.ErrorIs(t, set.ApplyDeletes([]int64{0}, mp), want)
	require.Nil(t, set.Readers())
	require.Zero(t, live)
}
