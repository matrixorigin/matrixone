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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestStringLengthWitnessSerializedGrowth(t *testing.T) {
	for _, depth := range []int{4, 8, 16, 32, 64} {
		for _, prepare := range []bool{false, true} {
			t.Run(fmt.Sprint(depth, "_", prepare), func(t *testing.T) {
				var sizes [2]int
				for i, length := range []string{"3", "ceil(2.1)"} {
					source := "@str_var"
					if prepare {
						source = "?"
					}
					for range depth {
						source = "left(" + source + "," + length + ")"
					}
					sql := "select " + source
					if prepare {
						sql = "prepare length_witness from '" + sql + "'"
					}
					p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
					require.NoError(t, err)
					sizes[i] = proto.Size(p)
					copied := DeepCopyPlan(p)
					require.True(t, proto.Equal(p, copied), "DeepCopy must retain every declaration fact")
					t.Logf("depth=%d SQL=%d bytes plan=%d bytes constant=%s", depth, len(sql), sizes[i], length)
				}
				require.LessOrEqual(t, sizes[1], sizes[0]*4+4096, "metadata must not duplicate executable child trees in protobuf")
			})
		}
	}
}

func BenchmarkStringLengthWitnessDeepCopy(b *testing.B) {
	for _, depth := range []int{8, 16, 32} {
		b.Run(fmt.Sprint(depth), func(b *testing.B) {
			source := "@str_var"
			for range depth {
				source = "left(" + source + ",ceil(2.1))"
			}
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(b)), b, "select "+source)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = DeepCopyPlan(p)
			}
		})
	}
}

func TestRegexpDeclarationClosure(t *testing.T) {
	for _, tc := range []struct {
		source   string
		mismatch bool
	}{
		{"cast(lpad(@str_var,-1,'x') as binary)", false},
		{"cast(rpad(@str_var,-1,'x') as binary)", false},
		{"cast(repeat(@str_var,-1) as binary)", false},
		{"cast(left(@str_var,round(2.8)) as binary)", true},
		{"cast(left(@str_var,if(true,3,4)) as binary)", true},
		{"cast(left(@str_var,case when true then 3 else 4 end) as binary)", true},
		{"(select coalesce(null,cast('abc' as binary)))", true},
		{"(select v from (select coalesce(null,cast('abc' as binary)) v) s)", true},
		{"max(coalesce(null,cast('abc' as binary)))", true},
		{"max(cast('abc' as binary)) over()", true},
		{"min(cast('abc' as binary)) over()", true},
		{"cast(coalesce(@n,0) as binary)", true},
		{"cast(ifnull(@n,0) as binary)", true},
		{"cast(case when true then @n else 0 end as binary)", true},
		{"cast(cast('abc' as char) as binary)", true},
		{"cast(cast(@str_var as char) as binary)", false},
		{"cast(cast(@str_var as char(0)) as binary)", true},
		{"cast(cast('' as char) as binary)", true},
		{"cast(cast(123 as char) as binary)", true},
		{"cast(concat(null,'abc') as binary)", true},
		{"cast(concat_ws(',',null,'abc') as binary)", true},
	} {
		sql := "select regexp_like(" + tc.source + ",'a')"
		for _, prepare := range []bool{false, true} {
			query := sql
			if prepare {
				query = "prepare regexp_closure from '" + strings.ReplaceAll(query, "'", "''") + "'"
			}
			t.Run(query, func(t *testing.T) {
				_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
				if tc.mismatch {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch), err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
