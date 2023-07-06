// Copyright 2023 Matrix Origin
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

package tree

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFmtCtx_WriteStringValue(t *testing.T) {
	tests := []struct {
		name   string
		fmtCtx *FmtCtx
		args   string
		want   string
	}{
		{
			name:   "test01",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreStringSingleQuotes|RestoreStringEscapeBackslash),
			args:   "123456789",
			want:   "'123456789'",
		},
		{
			name:   "test02",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreStringDoubleQuotes|RestoreStringEscapeBackslash),
			args:   "123456789",
			want:   "\"123456789\"",
		},
		{
			name:   "test03",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreStringDoubleQuotes|RestoreStringEscapeBackslash),
			args:   "12345\\6789",
			want:   "\"12345\\\\6789\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.fmtCtx
			ctx.WriteStringValue(tt.args)
			require.EqualValues(t, tt.want, ctx.ToString())
		})
	}
}

func TestFmtCtx_WriteName(t *testing.T) {
	tests := []struct {
		name   string
		fmtCtx *FmtCtx
		args   string
		want   string
	}{
		{
			name:   "test01",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "db1",
			want:   "`db1`",
		},
		{
			name:   "test02",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "employees",
			want:   "`employees`",
		},
		{
			name:   "test03",
			fmtCtx: NewFmtCtxWithFlag(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "col1",
			want:   "`col1`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.fmtCtx
			ctx.WriteName(tt.args)
			require.EqualValues(t, tt.want, ctx.ToString())
		})
	}
}
