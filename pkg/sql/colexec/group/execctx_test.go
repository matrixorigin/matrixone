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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInitGroupResultBuffer(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()

	{
		// pre-extend test.
		buf := GroupResultBuffer{}
		vec := []*vector.Vector{vector.NewVec(types.T_int64.ToType())}

		require.NoError(t, buf.InitWithGroupBy(mp, 256, nil, vec, 3*256+128))

		require.Equal(t, 3+1, cap(buf.ToPopped))

		ol := len(buf.ToPopped)
		buf.ToPopped = buf.ToPopped[:cap(buf.ToPopped)]
		require.Equal(t, 256, buf.ToPopped[0].Vecs[0].Capacity())
		require.Equal(t, 256, buf.ToPopped[1].Vecs[0].Capacity())
		require.Equal(t, 256, buf.ToPopped[2].Vecs[0].Capacity())
		require.Equal(t, 128, buf.ToPopped[3].Vecs[0].Capacity())
		buf.ToPopped = buf.ToPopped[:ol]

		buf.Free0(mp)
	}

	require.Equal(t, int64(0), mp.CurrNB())
}
