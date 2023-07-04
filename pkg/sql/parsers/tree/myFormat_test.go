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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"testing"
)

func TestFmtCtx_WriteStringValue(t *testing.T) {
	tests := []struct {
		name   string
		fmtCtx *FmtCtx
		args   string
	}{
		{
			name:   "test01",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreStringSingleQuotes|RestoreStringEscapeBackslash),
			args:   "123456789",
		},
		{
			name:   "test02",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreStringDoubleQuotes|RestoreStringEscapeBackslash),
			args:   "123456789",
		},
		{
			name:   "test03",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreStringDoubleQuotes|RestoreStringEscapeBackslash),
			args:   "12345\\6789",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.fmtCtx
			//ctx.WriteWithString(tt.args)
			ctx.WriteValue(P_char, tt.args)
			fmt.Println(ctx.ToString())
		})
	}
}

func TestFmtCtx_WriteName(t *testing.T) {
	tests := []struct {
		name   string
		fmtCtx *FmtCtx
		args   string
	}{
		{
			name:   "test01",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "db1",
		},
		{
			name:   "test02",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "employees",
		},
		{
			name:   "test03",
			fmtCtx: NewFmtCtx2(dialect.MYSQL, RestoreNameBackQuotes),
			args:   "col1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.fmtCtx
			ctx.WriteName(tt.args)
			fmt.Println(ctx.ToString())
		})
	}
}
