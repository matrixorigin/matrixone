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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDupTypePreservesEnumIdentity(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_enum), EnumEncoding: types.EnumEncodingFramed, EnumCollation: types.EnumCollationGeneralCI}
	got := dupType(&typ)
	require.Equal(t, types.T_enum, got.Oid)
	require.Equal(t, types.EnumEncodingFramed, got.GetEnumEncoding())
	require.Equal(t, types.EnumCollationGeneralCI, got.GetEnumCollation())
	scalar := plan.Type{Id: int32(types.T_int64), Width: 42, Scale: 3}
	require.Equal(t, types.New(types.T_int64, 42, 3), dupType(&scalar))
}
