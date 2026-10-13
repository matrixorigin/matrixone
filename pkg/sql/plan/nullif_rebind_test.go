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

package plan

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestInitNullMapRebindReturnsSameRules(t *testing.T) {
	for _, tc := range []struct {
		name, assignments string
		want              map[string][]string
	}{
		{"single", "a=nullif(a,'null')", map[string][]string{"a": {"null"}}},
		{"multiple sentinels", "a=nullif(a,'null'),a=nullif(a,'')", map[string][]string{"a": {"null", ""}}},
		{"multiple columns", "a=nullif(a,'null'),b=nullif(b,'nil')", map[string][]string{"a": {"null"}, "b": {"nil"}}},
		{"legacy unsupported same column boundary", "a=nullif(a,'null'),a=upper(a),b=nullif(b,'nil')", map[string][]string{"a": {"null"}}},
		{"legacy unsupported assignment boundary", "a=nullif(a,'null'),b=upper(b),a=nullif(a,'nil')", map[string][]string{"a": {"null"}}},
	} {
		for _, kind := range []string{"external", "load"} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				prefix := "create external table xt(a varchar(20),b varchar(20)) infile{'filepath'='ignored.csv'} fields terminated by ',' set "
				if kind == "load" {
					prefix = "load data infile 'ignored.csv' into table xt fields terminated by ',' set "
				}
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, prefix+tc.assignments, 1)
				require.NoError(t, err)
				defer stmt.Free()
				var param *tree.ExternParam
				if kind == "external" {
					param = stmt.(*tree.CreateTable).Param
				} else {
					param = stmt.(*tree.Load).Param
				}
				for pass := range 20 {
					require.NoError(t, InitNullMap(param, nil))
					require.Equal(t, tc.want, param.NullMap, "rebind %d", pass)
				}
				if strings.HasPrefix(tc.name, "legacy unsupported") {
					// Only the consumed prefix is supported by this binder; a
					// trailing, unconsumed expression has no JSON codec contract.
					return
				}
				// The stored external-table parameter must retain the same rules
				// after repeated binding and a remote JSON round trip.
				raw, err := json.Marshal(param)
				require.NoError(t, err)
				var decoded tree.ExternParam
				require.NoError(t, json.Unmarshal(raw, &decoded))
				require.Equal(t, tc.want, decoded.NullMap)
			})
		}
	}
}

func TestExternalNullMapSurvivesPlanRebuild(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create external table xt(a varchar(20),b varchar(20)) infile{'filepath'='ignored.csv'} fields terminated by ',' set a=nullif(a,'null'),a=nullif(a,''),b=nullif(b,'nil')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	for pass := range 20 {
		built, err := BuildPlan(&mock.ctxt, stmt, false)
		require.NoError(t, err)
		var stored tree.ExternParam
		var raw string
		for _, def := range built.GetDdl().GetCreateTable().GetTableDef().Defs {
			if properties := def.GetProperties(); properties != nil {
				for _, property := range properties.Properties {
					if property.Key == catalog.SystemRelAttr_CreateSQL {
						raw = property.Value
					}
				}
			}
		}
		require.NotEmpty(t, raw)
		require.NoError(t, json.Unmarshal([]byte(raw), &stored))
		require.Equal(t, map[string][]string{"a": {"null", ""}, "b": {"nil"}}, stored.NullMap, "plan rebuild %d", pass)
	}
}
