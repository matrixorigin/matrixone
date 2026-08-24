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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// This deliberately uses only pre-feature parser APIs so the identical test
// patch compiles on the pinned base and proves the syntax regression there.
func TestDMLReturningBaseRegression(t *testing.T) {
	for _, sql := range []string{
		"insert into t values (1) returning *",
		"update t set a = 2 returning a",
		"delete from t returning a",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}
