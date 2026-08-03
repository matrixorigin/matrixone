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

package lifecycle

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestExpirationClassifierKeepsSnapshotDeletesDisjoint(t *testing.T) {
	mp := mpool.MustNewZero()
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_date.ToType())
	defer value.Clean(mp)
	for _, date := range []types.Date{1, 2, 3, 4} {
		require.NoError(t, vector.AppendFixed(value.Vecs[0], date, false, mp))
	}
	value.SetRowCount(4)
	snapshotDeleted := &nulls.Nulls{}
	snapshotDeleted.Add(0)
	expired, err := (ExpirationClassifier{
		ColumnOrdinal: 0,
		ColumnType:    types.T_date,
		Cutoff:        3,
	}).Classify(context.Background(), value, snapshotDeleted)
	require.NoError(t, err)
	require.False(t, expired.Contains(0))
	require.True(t, expired.Contains(1))
	require.False(t, expired.Contains(2))
	require.False(t, expired.Contains(3))
}
