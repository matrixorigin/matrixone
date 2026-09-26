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

package v4_0_8

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRelease42ViewMetadataUpgrade(t *testing.T) {
	tables := []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH}
	ddls := []string{catalog.MoViewDependenciesDDL, catalog.MoViewRefreshDDL}
	for _, tc := range []struct {
		name    string
		present [2]bool
		fail    string
	}{
		{name: "release42_missing"},
		{name: "partial", present: [2]bool{true, false}},
		{name: "current", present: [2]bool{true, true}},
		{name: "check_error", fail: "check"},
		{name: "create_error", fail: "create"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				present := tc.present
				injected := errors.New("injected catalog failure")
				failed := false
				var creates []string
				txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
					require.False(t, failed, "must stop after an upgrade error")
					for i, table := range tables {
						if strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id") && strings.Contains(sql, "relname = '"+table+"'") {
							require.Contains(t, sql, "account_id = 0")
							if tc.fail == "check" {
								failed = true
								return executor.Result{}, injected
							}
							if present[i] {
								return statisticsStringResult(t, catalog.MO_CATALOG), nil
							}
							return executor.Result{}, nil
						}
						if sql == ddls[i] {
							require.False(t, present[i], "existing metadata must not be recreated")
							if tc.fail == "create" {
								failed = true
								return executor.Result{}, injected
							}
							present[i] = true
							creates = append(creates, table)
							return executor.Result{}, nil
						}
					}
					t.Fatalf("unexpected upgrade SQL (no drop/rewrite is allowed): %s", sql)
					return executor.Result{}, nil
				})
				err := Handler.HandleClusterUpgrade(t.Context(), txn)
				if tc.fail != "" {
					require.ErrorIs(t, err, injected)
					require.True(t, failed)
					return
				}
				require.NoError(t, err)
				require.Equal(t, [2]bool{true, true}, present)
				var want []string
				for i, exists := range tc.present {
					if !exists {
						want = append(want, tables[i])
					}
				}
				require.Equal(t, want, creates)
				creates = nil
				require.NoError(t, Handler.HandleClusterUpgrade(t.Context(), txn))
				require.Empty(t, creates, "retry must preserve existing table data")
			})
		})
	}
}
