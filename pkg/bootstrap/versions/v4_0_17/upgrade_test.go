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

package v4_0_17

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewUpgrade(t *testing.T) {
	require.Equal(t, "4.0.17", Handler.Metadata().Version)
	require.Equal(t, defines.MORPCVersion119, Handler.Metadata().RequiredProtocolVersion)
	for _, scenario := range []string{"ready", "DDL failure", "old CN", "protocol query failure"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			injected := errors.New("catalog creation failed")
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			tx := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				if sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')" {
					if scenario == "protocol query failure" {
						return executor.Result{}, injected
					}
					value := `{"result":"cn-a:119,cn-b:119"}`
					if scenario == "old CN" {
						value = `{"result":"cn-a:119,cn-b:118"}`
					}
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
					return r.GetResult(), nil
				}
				calls++
				if calls <= 4 {
					require.True(t, strings.HasPrefix(sql, "create cluster table if not exists mo_catalog.mo_"))
				} else {
					require.Contains(t, sql, "set interval_seconds=0,next_due=null,epoch=epoch+1")
					require.Contains(t, sql, "cast(json_extract(definition,'$.format_version') as int)=1")
				}
				if scenario == "DDL failure" && calls == 2 {
					return executor.Result{}, injected
				}
				return executor.Result{}, nil
			}, nil)
			err := Handler.HandleClusterUpgrade(context.Background(), tx)
			switch scenario {
			case "old CN":
				require.ErrorContains(t, err, "requires all CNs to support protocol version 119")
				require.Zero(t, calls, "old CN must block before the first catalog DDL")
			case "protocol query failure":
				require.ErrorIs(t, err, injected)
				require.Zero(t, calls)
			case "DDL failure":
				require.ErrorIs(t, err, injected)
				require.Equal(t, 2, calls)
			default:
				require.NoError(t, err)
				require.Equal(t, 5, calls)
			}
		})
	}
}
