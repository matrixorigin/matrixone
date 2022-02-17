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
	"github.com/stretchr/testify/require"
	"testing"
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
	myT := T(1)
	require.Equal(t, int32(1), myT.ToType().Size)
	myT = T(2)
	require.Equal(t, int32(2), myT.ToType().Size)
	myT = T(3)
	require.Equal(t, int32(4), myT.ToType().Size)
}

func TestT_String(t *testing.T) {
	myT := T(1)
	require.Equal(t, "TINYINT", myT.String())
	myT = T(2)
	require.Equal(t, "SMALLINT", myT.String())
	myT = T(3)
	require.Equal(t, "INT", myT.String())
}

func TestT_OidString(t *testing.T) {
	myT := T(1)
	require.Equal(t, "T_int8", myT.OidString())
	myT = T(2)
	require.Equal(t, "T_int16", myT.OidString())
}
