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

package external

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrepareTimeStagingPreservesOutputAllocation(t *testing.T) {
	for _, format := range []string{tree.CSV, tree.PARQUET} {
		for _, load := range []bool{false, true} {
			for _, accounted := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/load=%t/accounted=%t", format, load, accounted), func(t *testing.T) {
					proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
					defer proc.Free()
					param := &ExternalParam{
						ExParamConst: ExParamConst{
							Extern: &tree.ExternParam{
								ExParamConst: tree.ExParamConst{Format: format, ScanType: tree.INLINE, Tail: &tree.TailParameter{}},
							},
							Attrs: []plan.ExternAttr{{ColName: "v", ColIndex: 1}},
							Cols: []*plan.ColDef{
								{Name: "generated", Typ: plan.Type{Id: int32(types.T_int64)}},
								{Name: "v", Typ: plan.Type{Id: int32(types.T_time), Scale: 6}},
							},
						},
						ExParam: ExParam{Fileparam: &ExFileparam{}},
					}
					param.Extern.ExternType = int32(plan.ExternType_EXTERNAL_TB)
					if load {
						param.Extern.ExternType = int32(plan.ExternType_LOAD)
					}
					arg := NewArgument().WithEs(param)
					var account *mpool.AllocationAccount
					var registry *mpool.AllocationAccountRegistry
					var selection *vector.AllocationAccountSelection
					if accounted {
						var err error
						registry, err = mpool.NewAllocationAccountRegistry(1, 128)
						require.NoError(t, err)
						account, err = registry.Open(64 << 20)
						require.NoError(t, err)
						selection, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerExternal, 1, 2, 3, 4)
						require.NoError(t, err)
						arg.OutputAllocation = selection
					}
					defer func() {
						arg.Free(proc, false, nil)
						arg.OutputAllocation = nil
						arg.Release()
						if account != nil {
							require.Zero(t, account.Snapshot().Used)
							account.Seal()
							_, err := registry.Finalize(account)
							require.NoError(t, err)
						}
					}()
					require.NoError(t, arg.Prepare(proc))
					vec := arg.ctr.buf.Vecs[0]
					require.True(t, vector.AllocationAccountSelectionsEqual(selection, vec.AllocationAccountSelection()))
					if load && format == tree.CSV {
						require.Equal(t, types.T_varchar, vec.GetType().Oid)
						require.NoError(t, vector.AppendBytes(vec, []byte("838:59:59.999999"), false, proc.Mp()))
						require.Equal(t, "838:59:59.999999", vec.GetStringAt(0))
					} else {
						require.Equal(t, types.T_time, vec.GetType().Oid)
						require.NoError(t, vector.AppendFixed(vec, types.Time(1234567), false, proc.Mp()))
						require.Equal(t, []types.Time{1234567}, vector.MustFixedColWithTypeCheck[types.Time](vec))
					}
					if accounted {
						require.Positive(t, account.Snapshot().Used)
					}
				})
			}
		}
	}
}
