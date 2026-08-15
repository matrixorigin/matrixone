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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestValidateInsertColumnQualifiers(t *testing.T) {
	name := func(lower int64, parts ...string) *tree.UnresolvedName {
		cstrs := make([]*tree.CStr, len(parts))
		for i, part := range parts {
			cstrs[i] = tree.NewCStr(part, lower)
		}
		return tree.NewUnresolvedName(cstrs...)
	}

	tests := []struct {
		name       string
		columnName *tree.UnresolvedName
		lower      int64
		wantErr    string
	}{
		{name: "nil metadata"},
		{name: "unqualified", columnName: name(0, "id")},
		{name: "matching table", columnName: name(0, "t", "id")},
		{name: "matching database and table", columnName: name(0, "q1", "t", "id")},
		{name: "case insensitive mode", columnName: name(1, "Q1", "T", "id"), lower: 1},
		{name: "case sensitive table", columnName: name(0, "T", "id"), wantErr: "T.id"},
		{name: "case sensitive database", columnName: name(0, "Q1", "t", "id"), wantErr: "Q1.t.id"},
		{name: "wrong table", columnName: name(0, "other", "id"), wantErr: "other.id"},
		{name: "wrong database", columnName: name(0, "q2", "t", "id"), wantErr: "q2.t.id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateInsertColumnQualifiers(
				context.Background(), []*tree.UnresolvedName{test.columnName}, "q1", "t", test.lower,
			)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantErr)
			require.Contains(t, err.Error(), "field list")
		})
	}
}

func TestQualifiedInsertColumnName(t *testing.T) {
	name := func(parts ...string) *tree.UnresolvedName {
		cstrs := make([]*tree.CStr, len(parts))
		for i, part := range parts {
			cstrs[i] = tree.NewCStr(part, 1)
		}
		return tree.NewUnresolvedName(cstrs...)
	}

	require.Equal(t, "id", qualifiedInsertColumnName(name("id")))
	require.Equal(t, "t.id", qualifiedInsertColumnName(name("t", "id")))
	require.Equal(t, "q1.t.id", qualifiedInsertColumnName(name("q1", "t", "id")))
}
