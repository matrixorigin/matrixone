// Copyright 2021 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Empty CTE bodies (WITH a AS () ...) must be rejected at parse time, matching
// MySQL and avoiding a nil CTE.Stmt panic in Format (CTAS / PREPARE). See #30146.
func TestEmptyCTEBodyRejected(t *testing.T) {
	cases := []string{
		"with a as () select 1",
		"with a as () select 1 from dual",
		"create table t2 as with a as () select 1",
		"with recursive a as () select 1",
		"with a as (), b as (select 1) select * from b",
		"with a as (select 1), b as () select * from a",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, "empty CTE body must be a syntax error")
		})
	}
}

func TestNonEmptyCTEBodyStillParses(t *testing.T) {
	cases := []string{
		"with a as (select 1) select * from a",
		"with a as (select 1 as x) select x from a",
		"with a(x) as (select 1) select * from a",
		"with recursive a as (select 1 as n union all select n+1 from a where n < 3) select * from a",
		"create table t2 as with a as (select 1 as id) select * from a",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
		})
	}
}
