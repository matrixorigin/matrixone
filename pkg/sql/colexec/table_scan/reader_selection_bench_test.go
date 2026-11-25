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

package table_scan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkReaderSelect evaluates the reader's Select callback over an
// 8192-row block: one predicate (a > 450, 55% kept) or a chain of two
// (a > 450 then a < 800, 35% kept), with one row in 11 NULL.
func BenchmarkReaderSelectOne(b *testing.B) { benchReaderSelect(b, 1) }
func BenchmarkReaderSelectTwo(b *testing.B) { benchReaderSelect(b, 2) }

func benchReaderSelect(b *testing.B, predicates int) {
	t := &testing.T{}
	intType := types.T_int32.ToType()
	proc := testutil.NewProc(b)
	exprs := []*pbplan.Expr{lateTestCompare(t, ">", 0, 450)}
	if predicates == 2 {
		exprs = append(exprs, lateTestCompare(t, "<", 0, 800))
	}
	scan := &TableScan{
		Attrs:       []string{"a", "b"},
		Types:       []pbplan.Type{plan.MakePlan2Type(&intType), plan.MakePlan2Type(&intType)},
		FilterExprs: exprs,
	}
	require.NoError(b, scan.Prepare(proc))
	const rows = 8192
	bat := batch.NewWithSize(2)
	for c := range bat.Vecs {
		bat.Vecs[c] = vector.NewVec(intType, scan.mp(proc))
	}
	for i := 0; i < rows; i++ {
		require.NoError(b, vector.AppendFixed(bat.Vecs[0], int32((i*7919)%1000), i%11 == 0, scan.mp(proc)))
		require.NoError(b, vector.AppendFixed(bat.Vecs[1], int32(i), false, scan.mp(proc)))
	}
	bat.SetRowCount(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := scan.selectReaderFilter(proc, bat, []int{0}); err != nil {
			b.Fatal(err)
		}
	}
}
