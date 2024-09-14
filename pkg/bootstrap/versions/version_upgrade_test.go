// Copyright 2024 Matrix Origin
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

package versions

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_getVersionUpgradesBySQL(t *testing.T) {
	preSql := fmt.Sprintf(`select
			id,
			from_version,
			to_version,
			final_version,
			final_version_offset,
			state,
			upgrade_order,
			upgrade_cluster,
			upgrade_tenant,
			total_tenant,
			ready_tenant
			from %s
			where state = %d
			order by upgrade_order asc`,
		catalog.MOUpgradeTable,
		StateUpgradingTenant)

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if strings.EqualFold(sql, preSql) {
					typs := []types.Type{
						types.New(types.T_uint64, 64, 0),
						types.New(types.T_varchar, 50, 0),
						types.New(types.T_varchar, 50, 0),
						types.New(types.T_varchar, 50, 0),
						types.New(types.T_uint32, 32, 0),
						types.New(types.T_int32, 64, 0),
						types.New(types.T_int32, 64, 0),
						types.New(types.T_int32, 64, 0),
						types.New(types.T_int32, 64, 0),
						types.New(types.T_int32, 32, 0),
						types.New(types.T_int32, 32, 0),
					}

					memRes := executor.NewMemResult(typs, mpool.MustNewZero())
					memRes.NewBatchWithRowCount(2)

					executor.AppendFixedRows(memRes, 0, []uint64{10001, 10002})
					executor.AppendStringRows(memRes, 1, []string{"1.2.1", "1.2.2"})
					executor.AppendStringRows(memRes, 2, []string{"1.2.2", "1.2.3"})
					executor.AppendStringRows(memRes, 3, []string{"1.2.3", "1.2.3"})
					executor.AppendFixedRows(memRes, 4, []uint32{2, 2})
					executor.AppendFixedRows(memRes, 5, []int32{2, 2})
					executor.AppendFixedRows(memRes, 6, []int32{0, 1})
					executor.AppendFixedRows(memRes, 7, []int32{0, 1})
					executor.AppendFixedRows(memRes, 8, []int32{0, 1})
					executor.AppendFixedRows(memRes, 9, []int32{0, 238})
					executor.AppendFixedRows(memRes, 10, []int32{0, 0})

					result := memRes.GetResult()
					return result, nil
				}
				return executor.Result{}, nil
			}, txnOperator)

			_, err := getVersionUpgradesBySQL(preSql, executor)
			require.NoError(t, err)
		},
	)
}
