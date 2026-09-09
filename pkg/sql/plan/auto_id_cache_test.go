// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCachePlanAndPersistence(t *testing.T) {
	for _, size := range []uint64{0, 1, 2, 1000000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			sql := fmt.Sprintf("create table t(id bigint auto_increment primary key) auto_id_cache = %d", size)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.Contains(t, tree.String(stmt, dialect.MYSQL), fmt.Sprintf("auto_id_cache = %d", size))
			ctx := NewMockCompilerContext(false)
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			def := p.GetDdl().GetCreateTable().GetTableDef()
			require.Equal(t, size, def.AutoIdCache)
			require.Equal(t, size, DeepCopyTableDef(def, true).AutoIdCache)
			data, err := def.Marshal()
			require.NoError(t, err)
			var restored pb.TableDef
			require.NoError(t, restored.Unmarshal(data))
			require.Equal(t, size, restored.AutoIdCache)
			_, extra, err := engine.PlanDefsToExeDefs(def)
			require.NoError(t, err)
			require.Equal(t, size, api.CloneExtra(extra).AutoIdCache)
			require.Equal(t, size, api.MustUnmarshalTblExtra(api.MustMarshalTblExtra(extra)).AutoIdCache)
			require.Equal(t, size, incrservice.GetUserAutoColumnFromDef(def)[0].CacheSize)
			show, _, err := ConstructCreateTableSQL(ctx, def, nil, false, nil)
			require.NoError(t, err)
			if size == 0 {
				require.NotContains(t, show, "AUTO_ID_CACHE")
			} else {
				require.Contains(t, show, fmt.Sprintf("AUTO_ID_CACHE=%d", size))
			}
		})
	}
}

func TestAutoIDCacheZeroWithoutAutoColumn(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(id bigint) auto_id_cache=0", 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	require.Zero(t, p.GetDdl().GetCreateTable().GetTableDef().AutoIdCache)
}

func TestAutoIDCachePlanRejectsInvalidOptions(t *testing.T) {
	for _, sql := range []string{
		"create table t(id bigint auto_increment) auto_id_cache=1000001",
		"create table t(id bigint auto_increment) auto_id_cache=18446744073709551615",
		"create table t(id bigint auto_increment) auto_id_cache=1 auto_id_cache=2",
		"create table t(id bigint) auto_id_cache=1",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.ErrorContains(t, err, "AUTO_ID_CACHE")
		})
	}
	for _, value := range []string{"-1", "1.5", "18446744073709551616", "'one'"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(id int auto_increment) auto_id_cache="+value, 1)
		if stmt != nil {
			stmt.Free()
		}
		require.Error(t, err, value)
	}
}
