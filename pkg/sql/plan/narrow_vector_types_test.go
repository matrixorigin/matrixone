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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// ALTER TABLE ... ADD v VECF16(n) NOT NULL on a non-empty table synthesizes the
// backfill value. Enumerating vecf32/vecf64 only made the narrow types fall
// through to "null" — invalid for a NOT NULL column — where vecf32 produced a
// zero vector of the right width.
func TestBuildNotNullColumnValNarrowVectors(t *testing.T) {
	for _, oid := range []types.T{
		types.T_array_float32, types.T_array_float64, // parity baseline
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
	} {
		col := &ColDef{Typ: plan.Type{Id: int32(oid), Width: 3}}
		require.Equal(t, "'[0,0,0]'", buildNotNullColumnVal(col), "oid %s", oid.String())

		// width 0 degenerates to the empty vector, same as vecf32 does.
		col0 := &ColDef{Typ: plan.Type{Id: int32(oid), Width: 0}}
		require.Equal(t, "'[]'", buildNotNullColumnVal(col0), "oid %s width 0", oid.String())
	}
}

// Row-carrier costing treats vectors as varlen payloads. Omitting the narrow
// types made a vecbf16(768) column fall to the default varlen estimate instead
// of its real width — a plan-quality regression, not a wrong answer.
func TestRowCarrierCostNarrowVectors(t *testing.T) {
	for _, oid := range []types.T{
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
	} {
		got := getRowCarrierTypeCost(plan.Type{Id: int32(oid), Width: 768})
		require.Equal(t, int64(768), got.width, "oid %s should cost its declared width", oid.String())
	}
}
