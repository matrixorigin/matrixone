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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestInplaceSortAndCompactUsesNative0900Identity(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })

	ai := NewVec(types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB40900AI))
	require.NoError(t, AppendBytesList(ai, [][]byte{
		[]byte("a"), []byte("A"), []byte("á"),
	}, nil, mp))
	ai.InplaceSortAndCompact()
	require.Equal(t, 1, ai.Length())
	ai.Free(mp)

	bin := NewVec(types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB40900Bin))
	require.NoError(t, AppendBytesList(bin, [][]byte{
		[]byte("a "), []byte("a"),
	}, nil, mp))
	bin.InplaceSortAndCompact()
	require.Equal(t, 2, bin.Length())
	require.Equal(t, []byte("a"), bin.GetBytesAt(0))
	require.Equal(t, []byte("a "), bin.GetBytesAt(1))
	bin.Free(mp)
}
