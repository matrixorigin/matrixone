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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMockCompilerVariableDefaults(t *testing.T) {
	ctx := &MockCompilerContext{ctx: t.Context()}
	for _, global := range []bool{false, true} {
		value, err := ctx.ResolveVariable("div_precision_increment", true, global)
		require.NoError(t, err)
		require.Equal(t, int64(function.DefaultDivPrecisionIncrement), value)
	}
	_, err := ctx.ResolveVariable("div_precision_increment", false, false)
	require.ErrorContains(t, err, "var not found")
	value, err := ctx.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Equal(t, "ONLY_FULL_GROUP_BY", value)
	ctx.mysqlCompatible = true
	value, err = ctx.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Equal(t, "", value)
	override := "STRICT_TRANS_TABLES"
	ctx.sqlModeOverride = &override
	value, err = ctx.ResolveVariable("sql_mode", true, false)
	require.NoError(t, err)
	require.Equal(t, override, value)
	// An explicit compiler override owns resolution, including known defaults.
	readErr := errors.New("session read failed")
	ctx.ResolveVariableFunc = func(string, bool, bool) (any, error) { return nil, readErr }
	_, err = ctx.ResolveVariable("sql_mode", true, false)
	require.ErrorIs(t, err, readErr)
}

func BenchmarkMockCompilerResolveVariable(b *testing.B) {
	ctx := &MockCompilerContext{ctx: b.Context()}
	for _, name := range []string{"sql_mode", "int_var", "div_precision_increment", "decimal_var", "unknown_variable"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := ctx.ResolveVariable(name, true, false)
				if err != nil && name != "unknown_variable" {
					b.Fatal(err)
				}
			}
		})
	}
}
