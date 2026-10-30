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

	"github.com/stretchr/testify/require"
)

// IFNULL/IF/CASE accept a JSON value next to text or numeric values, as
// COALESCE already does, and IF accepts a JSON condition (issue #31544).
func TestConditionalFunctionsAcceptJSONBranches(t *testing.T) {
	for _, sql := range []string{
		`select ifnull(json_extract('{"a":null}', '$.a'), 'x')`,
		`select ifnull(json_extract('{"a":7}', '$.a'), 'x')`,
		`select ifnull(json_extract('{"a":1}', '$.zz'), 'x')`,
		`select ifnull(json_extract('{"a":1}', '$.zz'), 0)`,
		`select if(json_extract('{"a":null}', '$.a'), 'y', 'n')`,
		`select if(json_extract('{"a":7}', '$.a'), 1, 0)`,
		`select if(true, json_extract('{"a":7}', '$.a'), 'n')`,
		`select case when 1 = 1 then json_extract('{"a":7}', '$.a') else 'n' end`,
		`select coalesce(json_extract('{"a":null}', '$.a'), 'x')`,
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
		})
	}
}
