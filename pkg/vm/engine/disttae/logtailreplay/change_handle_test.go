// Copyright 2024 Matrix Origin
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

package logtailreplay

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func TestUpdateCNDataBatch_RemoveTSVector(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch with multiple vectors including T_TS type
	bat := batch.NewWithSize(3)

	// Create an int64 vector (non-TS)
	vec1 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec1, int64(1), false, mp)
	vector.AppendFixed(vec1, int64(2), false, mp)
	bat.Vecs[0] = vec1

	// Create a T_TS vector (should be removed)
	tsVec1 := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec1, types.BuildTS(1, 0), false, mp)
	vector.AppendFixed(tsVec1, types.BuildTS(2, 0), false, mp)
	bat.Vecs[1] = tsVec1

	// Create another int64 vector (non-TS)
	vec2 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec2, int64(3), false, mp)
	vector.AppendFixed(vec2, int64(4), false, mp)
	bat.Vecs[2] = vec2

	bat.SetRowCount(2)

	// Verify initial state: should have 3 vectors
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[2].GetType().Oid)

	// Call updateCNDataBatch
	newCommitTS := types.BuildTS(100, 0)
	updateCNDataBatch(bat, newCommitTS, mp)

	// Verify new commitTS vector is appended at the end (original vectors preserved)
	require.Equal(t, 4, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[2].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[3].GetType().Oid)

	// Verify the new commitTS vector has the correct value
	require.True(t, bat.Vecs[3].IsConst())
	tsVal := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[3])[0]
	require.Equal(t, newCommitTS, tsVal)

	bat.Clean(mp)
}

func TestUpdateCNDataBatch_NoTSVector(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a batch without T_TS vectors
	bat := batch.NewWithSize(2)

	vec1 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec1, int64(1), false, mp)
	vector.AppendFixed(vec1, int64(2), false, mp)
	bat.Vecs[0] = vec1

	vec2 := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec2, int64(3), false, mp)
	vector.AppendFixed(vec2, int64(4), false, mp)
	bat.Vecs[1] = vec2

	bat.SetRowCount(2)

	// Verify initial state: should have 2 vectors
	require.Equal(t, 2, len(bat.Vecs))

	// Call updateCNDataBatch
	newCommitTS := types.BuildTS(100, 0)
	updateCNDataBatch(bat, newCommitTS, mp)

	// Verify commitTS vector is added at the end
	require.Equal(t, 3, len(bat.Vecs))
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_int64, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[2].GetType().Oid)

	bat.Clean(mp)
}

func TestUpdateDataBatch_PreservesTrailingColumnsWithoutRowid(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	bat := batch.NewWithSize(4)
	bat.SetAttributes([]string{"id", "created_at", "updated_at", objectio.DefaultCommitTS_Attr})

	idVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(idVec, []byte("row-1"), false, mp))
	bat.Vecs[0] = idVec

	createdAt := vector.NewVec(types.New(types.T_datetime, 0, 6))
	createdAtVal, err := types.ParseDatetime("2026-03-12 19:18:00.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(createdAt, createdAtVal, false, mp))
	bat.Vecs[1] = createdAt

	updatedAt := vector.NewVec(types.New(types.T_datetime, 0, 6))
	updatedAtVal, err := types.ParseDatetime("2026-03-12 19:19:00.654321", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(updatedAt, updatedAtVal, false, mp))
	bat.Vecs[2] = updatedAt

	commitTS := vector.NewVec(types.T_TS.ToType())
	tsVal := types.BuildTS(100, 0)
	require.NoError(t, vector.AppendFixed(commitTS, tsVal, false, mp))
	bat.Vecs[3] = commitTS
	bat.SetRowCount(1)

	updateDataBatch(bat, types.BuildTS(50, 0), types.BuildTS(150, 0), mp)

	require.Equal(t, 4, len(bat.Vecs))
	require.Equal(t, []string{"id", "created_at", "updated_at", objectio.DefaultCommitTS_Attr}, bat.Attrs)
	require.Equal(t, types.T_varchar, bat.Vecs[0].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[1].GetType().Oid)
	require.Equal(t, types.T_datetime, bat.Vecs[2].GetType().Oid)
	require.Equal(t, types.T_TS, bat.Vecs[3].GetType().Oid)
	require.Equal(t, updatedAtVal, vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[2])[0])

	bat.Clean(mp)
}
