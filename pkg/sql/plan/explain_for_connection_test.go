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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// TestExplainForConnectionNotSupported covers #31747: EXPLAIN FOR CONNECTION
// is parsed but not implemented, so planning must report a clean
// not-supported error instead of an internal error that echoes a mangled
// "explain format =  for connection" statement.
func TestExplainForConnectionNotSupported(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, sql := range []string{
		"explain for connection 328",
		"explain format = 'json' for connection 999",
		"describe for connection 1",
	} {
		_, err := buildSingleStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%s: %v", sql, err)
		require.Contains(t, err.Error(), "EXPLAIN FOR CONNECTION", sql)
		require.NotContains(t, err.Error(), "format =  for", sql)
	}
}
