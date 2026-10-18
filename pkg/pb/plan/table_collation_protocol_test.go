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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableCollationFunctionFeature(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      int64
		display string
		want    bool
	}{
		{"execution_identity", 600, "", true},
		{"renamed_execution_identity", 600, "format_bytes", true},
		{"async_datalink_identity", 599, "internal_table_collation", false},
		{"calendar_interval_identity", 598, "internal_table_collation", false},
		{"format_bytes_control", 596, "internal_table_collation", false},
		{"format_pico_control", 597, "internal_table_collation", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &Expr{Typ: Type{Id: 61, Width: 64}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: tc.id << 32, ObjName: tc.display}, Args: []*Expr{{Typ: Type{Id: 61}, Expr: &Expr_Lit{Lit: &Literal{Value: &Literal_Sval{Sval: ""}}}}}}}}
			nested := &Expr{Typ: Type{Id: 10}, Expr: &Expr_F{F: &Function{Func: &ObjectRef{}, Args: []*Expr{expr}}}}
			owner := &TableDef{Cols: []*ColDef{{Default: &Default{Expr: nested}}}}
			data, err := owner.Marshal()
			require.NoError(t, err)
			var decoded TableDef
			require.NoError(t, decoded.Unmarshal(data))
			for _, input := range []any{expr, nested, owner, &decoded} {
				features, err := RequiredRemoteExpressionFeatures(input)
				require.NoError(t, err)
				require.Equal(t, tc.want, features.TableCollationFunction)
				require.Equal(t, tc.id == 598, features.CalendarIntervalNormalization, "calendar and table-collation identities are independent")
				require.Equal(t, tc.want || tc.id == 598, features.Any(), "either independent capability must activate pipeline admission")
			}
		})
	}
}
