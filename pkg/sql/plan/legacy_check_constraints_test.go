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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverLegacyCheckConstraints(t *testing.T) {
	checks, err := RecoverLegacyCheckConstraints(t.Context(), "create table old(a int check(a>0), b int, constraint named check(b<10) not enforced)")
	require.NoError(t, err)
	require.Equal(t, []LegacyCheckConstraint{
		{Name: "__mo_chk_1", Clause: "`a` > 0", Enforced: "YES"},
		{Name: "named", Clause: "`b` < 10", Enforced: "NO"},
	}, checks)
	for _, sql := range []string{"", "create table old(a int)", "select 'CHECK'"} {
		checks, err := RecoverLegacyCheckConstraints(t.Context(), sql)
		require.NoError(t, err)
		require.Empty(t, checks)
	}
	_, err = RecoverLegacyCheckConstraints(t.Context(), "create table CHECK invalid syntax")
	require.Error(t, err)
	_, err = RecoverLegacyCheckConstraints(t.Context(), `create table old(a int, check("a">0))`)
	require.ErrorContains(t, err, "ambiguous SQL mode")
}
