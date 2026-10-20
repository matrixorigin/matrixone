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

package mysql

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLoadInlineFormat(t *testing.T) {
	for _, tc := range []struct {
		format string
		want   string
	}{
		{"csv", tree.CSV}, {"CSV", tree.CSV}, {"cSv", tree.CSV},
		{"jsonline", tree.JSONLINE}, {"JSONLINE", tree.JSONLINE}, {"JsonLine", tree.JSONLINE},
		{"parquet", tree.PARQUET}, {"PARQUET", tree.PARQUET}, {"Parquet", tree.PARQUET},
		{"arrow", tree.ARROW}, {"ARROW", tree.ARROW}, {"Arrow", tree.ARROW},
		// Canonicalization must not introduce a default or trim invalid names.
		{"", ""}, {" ", " "}, {"CSV ", "csv "}, {" CSV", " csv"},
		{"Unknown", "unknown"}, {"JSON", "json"},
	} {
		t.Run(fmt.Sprintf("format=%q", tc.format), func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), fmt.Sprintf(
				`load data inline format='%s', data='MiXeD\nPayload', jsontype='ObJeCt' into table t fields terminated by ','`,
				tc.format), 1)
			require.NoError(t, err)
			defer stmt.Free()
			load := stmt.(*tree.Load)
			require.Equal(t, tree.INLINE, load.Param.ScanType)
			require.Equal(t, tc.want, load.Param.Format)
			require.Equal(t, "MiXeD\nPayload", load.Param.Data)
			require.Equal(t, "ObJeCt", load.Param.JsonData)
			require.Equal(t, ",", load.Param.Tail.Fields.Terminated.Value)
		})
	}
}
