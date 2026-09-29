// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataPKFilterReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := vector.NewVec(types.NewWithCharset(types.T_varchar, 8, 0, 3))
	require.NoError(t, vector.AppendBytes(source, []byte("x"), false, mp))
	source.SetSorted(true)
	wire, err := source.MarshalBinary()
	require.NoError(t, err)
	decoded, err := unmarshalPKInVector(wire)
	require.NoError(t, err)
	require.Equal(t, *source.GetType(), *decoded.GetType())
	decoded.Free(mp)
	wire[1+3] = 2
	decoded, err = unmarshalPKInVector(wire)
	require.ErrorContains(t, err, "collation")
	require.Nil(t, decoded)
	source.Free(mp)
	require.Zero(t, mp.CurrNB())
}
