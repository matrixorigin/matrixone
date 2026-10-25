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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// MySQL: "If there is no default database, DATABASE() returns NULL."
// SCHEMA() is a synonym and shares builtInDatabase.
func TestBuiltInDatabaseNullWithoutDefaultDatabase(t *testing.T) {
	cases := []struct {
		name     string
		database string
		want     string
		isNull   bool
	}{
		{name: "no default database", database: "", isNull: true},
		{name: "default database", database: "db1", want: "db1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.Database = c.database
			tcc := NewFunctionTestCase(proc, []FunctionTestInput{},
				NewFunctionTestResult(types.T_varchar.ToType(), false,
					[]string{c.want, c.want}, []bool{c.isNull, c.isNull}),
				builtInDatabase)
			tcc.fnLength = 2
			ok, info := tcc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
