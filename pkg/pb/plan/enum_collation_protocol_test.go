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

func TestRequiresEnumCollationProtocol(t *testing.T) {
	legacy := &TableDef{Cols: []*ColDef{{Typ: Type{Enumvalues: "a,b"}}}}
	require.False(t, RequiresEnumCollationProtocol(legacy))
	require.False(t, RequiresEnumCollationProtocol(nil))
	require.True(t, RequiresEnumCollationProtocol(&TableDef{DefaultCharset: 2}))
	require.True(t, RequiresEnumCollationProtocol(&TableDef{EnumUnsupportedDefault: "utf8mb4_unicode_ci"}))
	require.True(t, RequiresEnumCollationProtocol(&TableDef{Cols: []*ColDef{{Typ: Type{EnumCollation: 2}}}}))
	fn := &Function{Func: &ObjectRef{Obj: int64(306)<<32 | 2}, Args: make([]*Expr, 4)}
	require.True(t, RequiresEnumCollationProtocol(&Expr{Expr: &Expr_F{F: fn}}))
	fn.Args = fn.Args[:2]
	require.False(t, RequiresEnumCollationProtocol(&Expr{Expr: &Expr_F{F: fn}}))
}
