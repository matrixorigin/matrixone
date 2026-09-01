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

package partitionhash

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCompatibleUsesConservativeAllowlist(t *testing.T) {
	for _, typ := range []types.T{
		types.T_bool, types.T_int32, types.T_decimal128,
		types.T_timestamp, types.T_uuid, types.T_varchar,
	} {
		require.True(t, Compatible(typ), typ.String())
	}
	for _, typ := range []types.T{
		types.T_any, types.T_char, types.T_float64, types.T_json,
		types.T_array_float32, types.T_array_int8, types.T_tuple,
	} {
		require.False(t, Compatible(typ), typ.String())
	}
}
