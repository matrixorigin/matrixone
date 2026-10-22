// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativePADConversionPreservesAuthoredTarget(t *testing.T) {
	for _, tc := range []struct {
		name, source, target string
		identity, revision   uint32
		rejected             bool
	}{
		{"legacy bin alias", "utf32_bin", "utf8mb4 collate utf32_bin", 2, 0, false},
		{"legacy general alias", "utf32_bin", "utf8mb4 collate utf8mb4_0900_ai_ci", 3, 0, false},
		{"charset only", "utf32_bin", "utf8mb4", 3, 0, false},
		{"UCA to legacy", "utf8mb4_unicode_ci", "utf8mb4 collate utf32_bin", 2, 0, false},
		{"canonical bin is new", "utf32_bin", "utf8mb4 collate utf8mb4_bin", 2, 1, true},
		{"canonical general is new", "utf32_bin", "utf8mb4 collate utf8mb4_general_ci", 3, 1, true},
		{"native source cannot relabel", "utf8mb4_bin", "utf8mb4 collate utf32_bin", 2, 0, true},
		{"unchanged native", "utf8mb4_bin", "utf8mb4 collate utf8mb4_bin", 2, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table conversion_target(v varchar(8)) collate "+tc.source)
			source := mock.ctxt.tables["conversion_target"]
			before, err := source.Marshal()
			require.NoError(t, err)
			parent := mock.ctxt.GetContext()
			p, err := buildSingleStmt(mock, t, "alter table conversion_target convert to character set "+tc.target)
			after, marshalErr := source.Marshal()
			require.NoError(t, marshalErr)
			require.Equal(t, before, after, "conversion planning cannot mutate the source")
			require.Equal(t, parent, mock.ctxt.GetContext())
			if tc.rejected {
				require.ErrorContains(t, err, "online native PAD collation conversion is disabled")
				require.Nil(t, p)
				return
			}
			require.NoError(t, err)
			target := p.GetDdl().GetAlterTable().CopyTableDef
			col := FindColumn(target.Cols, "v")
			require.NotNil(t, col)
			require.Equal(t, tc.identity, col.Typ.Charset)
			require.Equal(t, tc.revision, col.Typ.CollationVersion)
			require.Equal(t, tc.identity, target.DefaultCharset)
			require.Equal(t, tc.revision, tableColumnCollationRevision(target))
		})
	}
}
