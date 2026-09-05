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

package jsonvalue

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFromVectorMySQLConstructorTypes(t *testing.T) {
	time0, err := types.ParseTime("04:05:06", 0)
	require.NoError(t, err)
	datetime0, err := types.ParseDatetime("2024-02-03 04:05:06", 0)
	require.NoError(t, err)
	timestamp0, err := types.ParseTimestamp(time.UTC, "2024-02-03 04:05:06", 0)
	require.NoError(t, err)
	time6, err := types.ParseTime("04:05:06.123456", 6)
	require.NoError(t, err)
	datetime6, err := types.ParseDatetime("2024-02-03 04:05:06.123456", 6)
	require.NoError(t, err)
	timestamp6, err := types.ParseTimestamp(time.UTC, "2024-02-03 04:05:06.123456", 6)
	require.NoError(t, err)
	plusEight := time.FixedZone("UTC+8", 8*60*60)

	tests := []struct {
		name     string
		typ      types.Type
		append   func(*vector.Vector, *mpool.MPool) error
		loc      *time.Location
		wantJSON string
		wantType string
	}{
		{
			name: "time scale zero", typ: types.New(types.T_time, 0, 0),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, time0, false, mp) },
			wantJSON: `"04:05:06.000000"`, wantType: "TIME",
		},
		{
			name: "datetime scale zero", typ: types.New(types.T_datetime, 0, 0),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, datetime0, false, mp) },
			wantJSON: `"2024-02-03 04:05:06.000000"`, wantType: "DATETIME",
		},
		{
			name: "timestamp scale zero", typ: types.New(types.T_timestamp, 0, 0), loc: plusEight,
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, timestamp0, false, mp) },
			wantJSON: `"2024-02-03 12:05:06.000000"`, wantType: "DATETIME",
		},
		{
			name: "time scale six", typ: types.New(types.T_time, 0, 6),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, time6, false, mp) },
			wantJSON: `"04:05:06.123456"`, wantType: "TIME",
		},
		{
			name: "datetime scale six", typ: types.New(types.T_datetime, 0, 6),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, datetime6, false, mp) },
			wantJSON: `"2024-02-03 04:05:06.123456"`, wantType: "DATETIME",
		},
		{
			name: "timestamp scale six UTC", typ: types.New(types.T_timestamp, 0, 6), loc: time.UTC,
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, timestamp6, false, mp) },
			wantJSON: `"2024-02-03 04:05:06.123456"`, wantType: "DATETIME",
		},
		{
			name: "timestamp default timezone", typ: types.New(types.T_timestamp, 0, 0),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, timestamp0, false, mp) },
			wantJSON: `"` + timestamp0.String2(time.Local, 6) + `"`, wantType: "DATETIME",
		},
		{
			name: "year", typ: types.T_year.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendFixed(v, types.MoYear(2024), false, mp)
			},
			wantJSON: `2024`, wantType: "INTEGER",
		},
		{
			name: "binary", typ: types.T_binary.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte{'A', 0, 0}, false, mp)
			},
			wantJSON: `"base64:type254:QQAA"`, wantType: "BLOB",
		},
		{
			name: "varbinary", typ: types.T_varbinary.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte{0, 0xff, 'A'}, false, mp)
			},
			wantJSON: `"base64:type15:AP9B"`, wantType: "BLOB",
		},
		{
			name: "blob", typ: types.T_blob.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte{0, 0xff, 'A'}, false, mp)
			},
			wantJSON: `"base64:type252:AP9B"`, wantType: "BLOB",
		},
		{
			name: "bit", typ: types.New(types.T_bit, 4, 0),
			append:   func(v *vector.Vector, mp *mpool.MPool) error { return vector.AppendFixed(v, uint64(10), false, mp) },
			wantJSON: `"base64:type16:Cg=="`, wantType: "BIT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := vector.NewVec(tc.typ)
			require.NoError(t, tc.append(v, mp))
			defer v.Free(mp)

			value, err := FromVector(context.Background(), v, 0, tc.loc, nil)
			require.NoError(t, err)
			bj, err := bytejson.CreateByteJSON(value)
			require.NoError(t, err)
			require.Equal(t, tc.wantJSON, bj.String())
			require.Equal(t, tc.wantType, bj.TYPE())
			if tc.name == "bit" {
				require.Equal(t, bytejson.TpCodeBlob, bj.Type)
			}
		})
	}
}

func TestFromVectorGeometryAndNull(t *testing.T) {
	mp := mpool.MustNewZero()
	geometry := vector.NewVec(types.T_geometry.ToType())
	require.NoError(t, vector.AppendBytes(geometry, []byte("wkb"), false, mp))
	defer geometry.Free(mp)

	value, err := FromVector(context.Background(), geometry, 0, time.UTC, func(payload []byte) (bytejson.ByteJson, error) {
		require.Equal(t, []byte("wkb"), payload)
		return bytejson.ParseFromString(`{"type":"Point","coordinates":[1,2]}`)
	})
	require.NoError(t, err)
	bj, err := bytejson.CreateByteJSON(value)
	require.NoError(t, err)
	require.Equal(t, bytejson.TpCodeObject, bj.Type)

	_, err = FromVector(context.Background(), geometry, 0, time.UTC, nil)
	require.ErrorContains(t, err, "geometry JSON conversion is unavailable")

	nulls := vector.NewVec(types.T_year.ToType())
	require.NoError(t, vector.AppendFixed(nulls, types.MoYear(0), true, mp))
	defer nulls.Free(mp)
	value, err = FromVector(context.Background(), nulls, 0, time.UTC, nil)
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestFromVectorRejectsInvalidBitWidth(t *testing.T) {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.New(types.T_bit, 65, 0))
	require.NoError(t, vector.AppendFixed(v, uint64(1), false, mp))
	defer v.Free(mp)

	_, err := FromVector(nil, v, 0, time.UTC, nil)
	require.ErrorContains(t, err, "cannot cast BIT(65) to json")
}
