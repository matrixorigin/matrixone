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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestRowIDReaderValuePreservesNull(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	defer mpool.DeleteMPool(mp)
	vec := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(vec, 0.0, true, mp))
	require.NoError(t, vector.AppendFixed(vec, 20.0, false, mp))

	require.Nil(t, rowIDReaderValue(vec, 0))
	require.Equal(t, 20.0, rowIDReaderValue(vec, 1))
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}
