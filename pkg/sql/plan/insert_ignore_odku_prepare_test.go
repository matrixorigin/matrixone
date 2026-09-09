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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedNoKeyODKUParameters(t *testing.T) {
	for _, modifier := range []string{"", "ignore "} {
		for _, rhs := range []string{"?", "? + 1", "1 + ?", "? + ?", "coalesce(?, 1)"} {
			t.Run(modifier+rhs, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				sql := fmt.Sprintf(
					"prepare s from insert %sinto insert_fk_no_key_c values (?, ?) on duplicate key update pid = %s",
					modifier,
					rhs,
				)
				p, err := runOneStmt(mock, t, sql)
				require.NoError(t, err)

				expected := 3
				if rhs == "? + ?" {
					expected = 4
				}
				require.Equal(t, expected, len(p.GetDcl().GetPrepare().GetParamTypes()))
			})
		}
	}
}
