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

package colexec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestFormatDedupKeyDecodesFloatIdentity(t *testing.T) {
	pool := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()

	packer.EncodeFloat32(1)
	float32Key := append([]byte(nil), packer.Bytes()...)
	packer.Reset()
	packer.EncodeFloat64(math.Copysign(0, -1))
	float64Key := append([]byte(nil), packer.Bytes()...)

	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(pool)
	require.NoError(t, vector.AppendBytes(vec, float32Key, false, pool))
	require.NoError(t, vector.AppendBytes(vec, float64Key, false, pool))

	got, err := FormatDedupKey(vec, 0, []plan.Type{{Id: int32(types.T_float32)}})
	require.NoError(t, err)
	require.Equal(t, "1", got)

	got, err = FormatDedupKey(vec, 1, []plan.Type{{Id: int32(types.T_float64)}})
	require.NoError(t, err)
	require.Equal(t, "-0", got)
}
