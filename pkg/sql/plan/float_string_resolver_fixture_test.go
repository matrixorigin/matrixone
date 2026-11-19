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
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// These persisted bindings use the same historical CREATE owner and frozen
// session values as the scope-session regressions. Both real resolver packages
// consume the wire fixture, so their measurements use identical source trees.
func legacyFloatResolverFixtures(t *testing.T) map[string][]byte {
	const division = "cast(1 as decimal(10,0))/cast(7 as decimal(10,0))"
	const full = "-0.0000000000000012345678901234568"
	fixtures := make(map[string][]byte)
	for _, tc := range []struct {
		name, expression, want string
		precision              int32
		offset                 int
	}{
		{"ordinary", "'ordinary'", "ordinary", 4, 8 * 3600},
		{"temporal_flat", "concat(a,from_unixtime(0))", full + "1970-01-01 08:00:00", 4, 8 * 3600},
		{"temporal_locale30", "concat(a,date_format(from_unixtime(0),'%M %H')," + division + ")", full + "Januar 080.142857142857142857142857142857", 30, 8 * 3600},
		{"nested_precision8", "concat(a,cast(concat(from_unixtime(120)," + division + ") as char(100)))", full + "1970-01-01 08:04:000.14285714", 8, 8*3600 + 2*60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler, source := legacyFloatScopeTable(t, "create table t(a double,g varchar(200) default("+tc.expression+"))", tc.precision, time.FixedZone("creation", tc.offset))
			source.DbName, source.TblId, source.Version = "db", 42, 1
			before := proto.Clone(source)
			execution := CloneTableDefForPlan(source, true)
			require.NoError(t, MigrateLegacyFloatStringTableDef(compiler, execution))
			require.Equal(t, tc.want, executeLegacyFloatScope(t, compiler, execution))
			require.True(t, proto.Equal(before, source), "source catalog must remain unchanged")
			require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
			require.Same(t, time.UTC, compiler.GetProcess().GetSessionInfo().TimeZone)
			var err error
			fixtures[tc.name+"_source"], err = source.Marshal()
			require.NoError(t, err)
			fixtures[tc.name+"_execution"], err = execution.Marshal()
			require.NoError(t, err)
		})
	}
	return fixtures
}

func TestLegacyFloatResolverFixtures(t *testing.T) {
	actual := legacyFloatResolverFixtures(t)
	data, err := os.ReadFile("testdata/legacy_float_resolver.json")
	require.NoError(t, err)
	var expected map[string][]byte
	require.NoError(t, json.Unmarshal(data, &expected))
	require.Equal(t, len(expected), len(actual))
	for name, wire := range expected {
		a, e := new(planpb.TableDef), new(planpb.TableDef)
		require.NoError(t, a.Unmarshal(actual[name]))
		require.NoError(t, e.Unmarshal(wire))
		require.True(t, proto.Equal(a, e), name)
	}
}
