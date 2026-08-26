// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ioutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func TestTombstoneColumnGuardMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	_, err := ValidateTombstoneRowIDColumn(-1, nil)
	require.Error(t, err)
	_, err = ValidateTombstoneRowIDColumn(0, nil)
	require.Error(t, err)
	wrongRowID := vector.NewVec(types.T_int64.ToType())
	defer wrongRowID.Free(mp)
	_, err = ValidateTombstoneRowIDColumn(0, wrongRowID)
	require.Error(t, err)
	constRowID, err := vector.NewConstFixed(types.T_Rowid.ToType(), types.Rowid{}, 2, mp)
	require.NoError(t, err)
	defer constRowID.Free(mp)
	_, err = ValidateTombstoneRowIDColumn(2, constRowID)
	require.Error(t, err)

	_, err = ValidateTombstoneAbortColumn(-1, nil)
	require.Error(t, err)
	aborts, err := ValidateTombstoneAbortColumn(3, nil)
	require.NoError(t, err)
	require.False(t, aborts.IsPresent())

	_, err = ValidateTombstoneCommitTSColumn(-1, nil)
	require.Error(t, err)
	_, err = ValidateTombstoneCommitTSColumn(0, nil)
	require.Error(t, err)
	wrongCommit := vector.NewVec(types.T_int64.ToType())
	defer wrongCommit.Free(mp)
	_, err = ValidateTombstoneCommitTSColumn(0, wrongCommit)
	require.Error(t, err)
}

func TestReadDeletesRejectsShortCacheBeforeIO(t *testing.T) {
	_, release, err := ReadDeletes(
		context.Background(), objectio.Location{}, nil, false,
		make(containers.Vectors, 2), nil,
	)
	require.ErrorContains(t, err, "cache has 2 slots")
	require.Nil(t, release)

	_, release, err = ReadDeletes(
		context.Background(), objectio.Location{}, nil, true,
		nil, nil,
	)
	require.ErrorContains(t, err, "cache has 0 slots")
	require.Nil(t, release)
}
