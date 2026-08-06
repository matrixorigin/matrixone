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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestFillIncludeResult pins the covering side channel: Search maps a covered query's
// RequestedIncludeColumns (by name, any order/subset) to each result's positional Include
// values, populating rt.IncludeResult.Data/Nulls — so the TVF emits the projected INCLUDE
// columns with no base-table JOIN. NULL values are flagged.
func TestFillIncludeResult(t *testing.T) {
	s := &Fulltext2Search{cfg: TableConfig{IncludeColumns: []string{"status", "prio"}}}
	results := []Result{
		{Pk: int64(1), Score: 1, Include: []any{[]byte("active"), int64(10)}},
		{Pk: int64(2), Score: 1, Include: []any{nil, int64(20)}}, // NULL status
	}

	ir := &vectorindex.IvfIncludeResult{}
	// Request in a different order + a subset (prio, then status).
	rt := vectorindex.RuntimeConfig{RequestedIncludeColumns: []string{"prio", "status"}, IncludeResult: ir}
	s.fillIncludeResult(rt, results)

	require.Equal(t, []any{int64(10), int64(20)}, ir.Data["prio"])
	require.Equal(t, []bool{false, false}, ir.Nulls["prio"])
	require.Equal(t, []any{[]byte("active"), nil}, ir.Data["status"])
	require.Equal(t, []bool{false, true}, ir.Nulls["status"]) // pk2 status is NULL

	// No requested columns → no-op (no panic, IncludeResult untouched).
	empty := &vectorindex.IvfIncludeResult{}
	s.fillIncludeResult(vectorindex.RuntimeConfig{IncludeResult: empty}, results)
	require.Nil(t, empty.Data)
}
