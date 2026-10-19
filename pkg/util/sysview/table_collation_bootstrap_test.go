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

package sysview

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestBootstrapDefersTableCollationView(t *testing.T) {
	var statements []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		statements = append(statements, sql)
		return executor.Result{}, nil
	}, nil)
	require.NoError(t, InitSchema(t.Context(), txn))
	require.Contains(t, statements, InformationSchemaTablesDDL)
	require.NotContains(t, statements, InformationSchemaTablesV118DDL)
	require.Contains(t, InitInformationSchemaSysTables, InformationSchemaTablesV118DDL, "ordinary tenant initialization retains the current definition")
}
