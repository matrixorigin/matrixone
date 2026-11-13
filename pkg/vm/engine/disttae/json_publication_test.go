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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShardingReadPublishesReceivedJSONVersion(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer input.Clean(mp)
	value, err := types.ParseStringToByteJson(`{"keep":1}`)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
	input.SetRowCount(1)
	encoded, err := input.MarshalBinary()
	require.NoError(t, err)
	length := uint32(len(encoded))
	response := append([]byte{0}, types.EncodeUint32(&length)...)
	response = append(response, encoded...)
	output := batch.NewWithSize(0)
	defer output.Clean(mp)
	publication := objectio.NewJSONReadSet([]int{0})
	defer publication.Close()
	ctx := objectio.WithJSONReadSet(context.Background(), publication)
	end, err := decodeShardingReadBatch(ctx, response, output, mp)
	require.NoError(t, err)
	require.False(t, end)
	reader := publication.Readers()[0]
	require.NotNil(t, reader)
	// Mutating received descriptors or payload cannot change the private version.
	changed, err := types.ParseStringToByteJson(`{"keep":9}`)
	require.NoError(t, err)
	raw, err := changed.Marshal()
	require.NoError(t, err)
	require.Equal(t, len(raw), copy(output.Vecs[0].GetBytesAt(0), raw))
	clear(response)
	output.Clean(mp)
	path, err := types.ParseStringToPath("$.keep")
	require.NoError(t, err)
	result, err := reader.Query(0, &path)
	require.NoError(t, err)
	defer result.Close()
	text, err := result.Text()
	require.NoError(t, err)
	require.Equal(t, "1", text.Text)
	text.Close()
	for _, response := range [][]byte{nil, {0}, {0, 1, 0, 0, 0}} {
		_, err := decodeShardingReadBatch(ctx, response, output, mp)
		require.Error(t, err)
	}
}
