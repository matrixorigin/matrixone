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

package v4_0_8

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestPublicationDatabaseIndexUpgrade(t *testing.T) {
	entry := clusterUpgEntries[len(clusterUpgEntries)-1]
	require.Equal(t, versions.ADD_INDEX, entry.UpgType)
	require.Equal(t, catalog.MO_PUBS, entry.TableName)

	for _, tc := range []struct {
		name    string
		invalid bool
	}{
		{name: "valid_catalog"},
		{name: "invalid_named_reference", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				indexExists := false
				indexCreates := 0
				txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "select 1 from mo_catalog.mo_pubs p "):
						if tc.invalid {
							return statisticsStringResult(t, "orphan"), nil
						}
						return executor.Result{}, nil
					case strings.HasPrefix(sql, "select distinct `idx`.`name`"):
						if indexExists {
							return statisticsStringResult(t, "idx_mo_pubs_database_id"), nil
						}
						return executor.Result{}, nil
					case sql == entry.UpgSql:
						indexExists = true
						indexCreates++
						return executor.Result{}, nil
					default:
						t.Fatalf("unexpected upgrade SQL: %s", sql)
						return executor.Result{}, nil
					}
				})
				if tc.invalid {
					require.ErrorContains(t, entry.Upgrade(txn, catalog.System_Account), "repair publication catalog")
					require.Zero(t, indexCreates)
					return
				}
				require.NoError(t, entry.Upgrade(txn, catalog.System_Account))
				require.NoError(t, entry.Upgrade(txn, catalog.System_Account))
				require.Equal(t, 1, indexCreates)
			})
		})
	}
}
