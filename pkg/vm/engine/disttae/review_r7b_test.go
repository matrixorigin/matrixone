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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestOrderedLocalScanSkipsColumnPrefetch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit uint64
	}{
		{name: "unlimited_ordered_scan"},
		{name: "ordered_scan_with_limit", limit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, fs, _ := newColumnPrefetchSource(t, true)
			defer ls.Close()
			typ := types.T_int32.ToType()
			ls.table = &txnTable{db: &txnDatabase{}, tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(typ.Oid)}, Seqnum: 0}},
			}}
			ls.Limit = tc.limit
			ls.SetOrderBy([]*plan.OrderBySpec{{Expr: &plan.Expr{
				Typ:  plan.Type{Id: int32(typ.Oid)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k"}},
			}}})
			ctx := fileservice.WithFileServicePolicy(context.Background(), fileservice.ColumnPrefetch|fileservice.SkipFullFilePreloads)
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			out := batch.NewWithSize(1)
			defer out.Clean(mp)
			block, state, err := ls.Next(ctx, []string{"k"}, []types.Type{typ}, []uint16{0}, 0, &readutil.MemPKFilter{}, mp, out)
			require.NoError(t, err)
			require.Equal(t, engine.Persisted, state)
			require.NotNil(t, block)
			require.Empty(t, fs.reqs, "an ordered scan should issue zero column prefetches, got %d", len(fs.reqs))
		})
	}
}
