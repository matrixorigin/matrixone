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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// A dependent table scan reads with its node's read policy: the context of its
// resolution, ranges and readers carries the policy, and so does its operator.
func TestDependentSourceAppliesScanReadPolicy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		policy    fileservice.Policy
		tableType string
		want      fileservice.Policy
	}{
		{"node policy", fileservice.SkipFullFilePreloads, "", fileservice.SkipFullFilePreloads},
		{"ivf entries", fileservice.ColumnPrefetch | fileservice.SkipFullFilePreloads,
			catalog.SystemSI_IVFFLAT_TblType_Entries, fileservice.IndexFileObject},
		{"control", 0, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer p.Free()
			budget, err := p.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<26, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			allocation, err := p.Mp().WithAccount(account, mpool.AllocationOwnerDependent, 1, mpool.AllocationCapacityClassDefault)
			require.NoError(t, err)

			ctrl := gomock.NewController(t)
			var seen []fileservice.Policy
			record := func(ctx context.Context) { seen = append(seen, fileservice.GetFileServicePolicy(ctx)) }
			reader := mock_frontend.NewMockReader(ctrl)
			reader.EXPECT().Close().Return(nil).AnyTimes()
			rel := mock_frontend.NewMockRelation(ctrl)
			rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ engine.RangesParam) (engine.RelData, error) {
					record(ctx)
					return nil, nil
				})
			rel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ any, _ *plan.Expr, _ engine.RelData, _, _ int, _ bool, _ engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
					record(ctx)
					return []engine.Reader{reader}, nil
				})
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ string, _ any) (engine.Relation, error) {
					record(ctx)
					return rel, nil
				})
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ string, _ any) (engine.Database, error) {
					record(ctx)
					return db, nil
				})

			typ := types.T_int32.ToType().PlanType()
			n := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ReadPolicy: uint64(tc.policy),
				ObjRef:   &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
				TableDef: &plan.TableDef{Name: "t", TableType: tc.tableType, Cols: []*plan.ColDef{{Name: "k", Typ: typ}}},
			}
			c := &Compile{proc: p, e: eng}
			factory, err := c.compileDependentSource(n)
			require.NoError(t, err)
			source, err := factory(p, allocation, account)
			require.NoError(t, err)
			defer source.Close()
			require.Equal(t, []fileservice.Policy{tc.want, tc.want, tc.want, tc.want}, seen)
			op, ok := source.(*dependentNativeSource).op.(*table_scan.TableScan)
			require.True(t, ok)
			require.Equal(t, tc.want, op.ReadPolicy)
			require.NoError(t, source.Close())
		})
	}
}
