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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The M < D check runs inside the type rule, which does not know the column
// (and a CAST target has none), so the error must not name a column at all.
// It used to always report a hard-coded column 'a'.
func TestFloatDecimalScaleAbovePrecisionErrorNamesNoColumn(t *testing.T) {
	cases := []string{
		"create table t (price decimal(5,6))",
		"create table t (price numeric(2,3))",
		"create table t (ratio float(10,11))",
		"create table t (ratio double(3,4))",
		"create table t (ratio double precision(3,4))",
		"alter table t add column amount decimal(10,11)",
		"select cast(1 as decimal(10,11))",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), "For float(M,D), double(M,D) or decimal(M,D), M must be >= D")
			require.NotContains(t, err.Error(), "(column 'a')")
			require.NotContains(t, err.Error(), "M must be >= D)")
		})
	}
}
