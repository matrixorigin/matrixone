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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestIndexOptionOpTypeLiteral(t *testing.T) {
	for _, tc := range []struct {
		name, value, want string
	}{
		{"operator", "vector_l2_ops", " OP_TYPE 'vector_l2_ops' "},
		{"quotes", "bad'\"operator", " OP_TYPE 'bad''\"operator' "},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			option := &IndexOption{AlgoParamVectorOpType: tc.value}
			for _, opts := range [][]FmtCtxOption{
				nil, {WithQuoteString(true)}, {WithSingleQuoteString()},
			} {
				require.Equal(t, tc.want, StringWithOpts(option, dialect.MYSQL, opts...))
			}
		})
	}
}
