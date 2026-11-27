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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestProtoVectorEnumCollationAdmission(t *testing.T) {
	vector := NewVec(types.T_enum.ToType())
	wire, err := VectorToProtoVector(vector)
	require.NoError(t, err)
	for _, id := range []uint32{0, 2, 3} {
		wire.Type.EnumCollation = id
		decoded, err := ProtoVectorToVector(wire)
		require.NoError(t, err)
		require.Equal(t, id, decoded.GetType().GetEnumCollation())
	}
	for _, id := range []uint32{1, 255, 256, 258} {
		wire.Type.EnumCollation = id
		_, err := ProtoVectorToVector(wire)
		require.Error(t, err, "invalid identity %d", id)
	}
	wire.Type.Id = int32(types.T_int32)
	wire.Type.EnumCollation = 2
	_, err = ProtoVectorToVector(wire)
	require.ErrorContains(t, err, "non-ENUM vector")
	wire.Type.Id = int32(types.T_enum)
	wire.Type.EnumCollation = 0
	wire.Type.EnumEncoding = 256
	_, err = ProtoVectorToVector(wire)
	require.Error(t, err)
}
