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

package v4_0_7

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestEnsureViewMetadataCatalogs(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		present := make(map[string]bool)
		ddls := map[string]string{
			catalog.MO_VIEW_DEPENDENCIES: catalog.MoViewDependenciesDDL,
			catalog.MO_VIEW_REFRESH:      catalog.MoViewRefreshDDL,
		}
		var creates []string
		var failDDL string
		failure := errors.New("injected DDL failure")
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			for name, ddl := range ddls {
				if strings.HasPrefix(sql, "SELECT reldatabase") && strings.Contains(sql, "relname = '"+name+"'") {
					if present[name] {
						return newProtocolVersionResultValue(t, name), nil
					}
					return executor.Result{}, nil
				}
				if sql == ddl {
					if name == failDDL {
						return executor.Result{}, failure
					}
					creates = append(creates, name)
					present[name] = true
					return executor.Result{}, nil
				}
			}
			return executor.Result{}, errors.New("unexpected SQL: " + sql)
		})

		failDDL = catalog.MO_VIEW_REFRESH
		require.ErrorIs(t, EnsureViewMetadataCatalogs(txn), failure)
		require.Equal(t, []string{catalog.MO_VIEW_DEPENDENCIES}, creates)
		// The real caller rolls back the transaction on failure. A subsequent
		// check also handles a pre-existing prerequisite without replacing it.
		failDDL = ""
		require.NoError(t, EnsureViewMetadataCatalogs(txn))
		require.NoError(t, EnsureViewMetadataCatalogs(txn))
		require.Equal(t, []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH}, creates)
	})
}
