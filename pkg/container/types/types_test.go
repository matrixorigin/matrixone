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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestType_String(t *testing.T) {
	myType := Type{Oid: T_int64, Size: 8}
	require.Equal(t, "BIGINT", myType.String())
}

func TestType_Eq(t *testing.T) {
	myType := Type{Oid: T_int64, Size: 8}
	myType1 := Type{Oid: T_int64, Size: 8}
	require.True(t, myType.Eq(myType1))
}

func TestT_ToType(t *testing.T) {
	require.Equal(t, int32(1), T_int8.ToType().Size)
	require.Equal(t, int32(2), T_int16.ToType().Size)
	require.Equal(t, int32(4), T_int32.ToType().Size)
	require.Equal(t, int32(8), T_int64.ToType().Size)
	require.Equal(t, int32(1), T_uint8.ToType().Size)
	require.Equal(t, int32(2), T_uint16.ToType().Size)
	require.Equal(t, int32(4), T_uint32.ToType().Size)
	require.Equal(t, int32(8), T_uint64.ToType().Size)
}

func TestT_String(t *testing.T) {
	require.Equal(t, "TINYINT", T_int8.String())
	require.Equal(t, "SMALLINT", T_int16.String())
	require.Equal(t, "INT", T_int32.String())
}

func TestT_OidString(t *testing.T) {
	require.Equal(t, "T_int8", T_int8.OidString())
	require.Equal(t, "T_int16", T_int16.OidString())
	require.Equal(t, "T_int32", T_int32.OidString())
	require.Equal(t, "T_int64", T_int64.OidString())
}
