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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestOrderFulltextTermsByMetadataUpperBound(t *testing.T) {
	infos := []*plan.MetadataScanInfo{
		{RowCnt: 500, ZoneMap: index.BuildZM(types.T_varchar, []byte("common"))},
		{RowCnt: 10, ZoneMap: index.BuildZM(types.T_varchar, []byte("rare"))},
	}
	ordered, rows, fallback := orderFulltextTerms([]string{"common", "rare"}, infos)
	require.Equal(t, []string{"rare", "common"}, ordered)
	require.Equal(t, uint64(10), rows)
	require.Zero(t, fallback)
}

func TestOrderFulltextTermsMetadataFallbackIsDeterministic(t *testing.T) {
	ordered, rows, fallback := orderFulltextTerms([]string{"aa", "bbbb", "ab"}, nil)
	require.Equal(t, []string{"bbbb", "aa", "ab"}, ordered)
	require.Zero(t, rows)
	require.Equal(t, int64(3), fallback)
}

func TestSplitFulltextTableName(t *testing.T) {
	db, table, err := splitFulltextTableName("`db`.`idx`")
	require.NoError(t, err)
	require.Equal(t, "db", db)
	require.Equal(t, "idx", table)
	_, _, err = splitFulltextTableName("idx")
	require.Error(t, err)
}
