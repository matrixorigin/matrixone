// Copyright 2021 Matrix Origin
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

package blockio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestFillCNCreatedCommitTSIfNeeded(t *testing.T) {
	mp := mpool.MustNewZero()
	cacheVectors := containers.NewVectors(1)
	defer cacheVectors.Free(mp)

	nullVec := vector.NewConstNull(types.T_TS.ToType(), 4, mp)
	cacheVectors[0] = *nullVec

	expected := types.BuildTS(123, 7)
	err := fillCNCreatedCommitTSIfNeeded(
		[]uint16{objectio.SEQNUM_COMMITTS},
		cacheVectors,
		mp,
		&objectio.BlockInfo{},
		func() (types.TS, bool) {
			return expected, true
		},
	)
	require.NoError(t, err)

	commitVec := &cacheVectors[0]
	require.False(t, commitVec.AllNull())
	require.Equal(t, 4, commitVec.Length())
	require.Equal(t, expected, vector.GetFixedAtNoTypeCheck[types.TS](commitVec, 0))
	require.Equal(t, expected, vector.GetFixedAtNoTypeCheck[types.TS](commitVec, 3))
}

func TestFillCNCreatedCommitTSIfNeededKeepExistingValue(t *testing.T) {
	mp := mpool.MustNewZero()
	cacheVectors := containers.NewVectors(1)
	defer cacheVectors.Free(mp)

	existing := types.BuildTS(100, 1)
	existingVec, err := vector.NewConstFixed(types.T_TS.ToType(), existing, 3, mp)
	require.NoError(t, err)
	cacheVectors[0] = *existingVec

	err = fillCNCreatedCommitTSIfNeeded(
		[]uint16{objectio.SEQNUM_COMMITTS},
		cacheVectors,
		mp,
		&objectio.BlockInfo{},
		func() (types.TS, bool) {
			return types.BuildTS(200, 2), true
		},
	)
	require.NoError(t, err)

	commitVec := &cacheVectors[0]
	require.Equal(t, existing, vector.GetFixedAtNoTypeCheck[types.TS](commitVec, 0))
}
