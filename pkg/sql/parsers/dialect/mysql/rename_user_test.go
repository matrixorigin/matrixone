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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRenameUserGrammar(t *testing.T) {
	for _, sql := range []string{"rename user a to b", "rename user 'a'@'%' to 'b'@'localhost', b to c", `rename user 'o''brien' to 'next\\name'`} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		r := stmt.(*tree.RenameUser)
		require.NotEmpty(t, r.Pairs)
		out := tree.String(r, dialect.MYSQL)
		again, err := ParseOne(context.Background(), out, 1)
		require.NoError(t, err)
		require.Equal(t, out, tree.String(again, dialect.MYSQL))
		stmt.Free()
		again.Free()
	}
	for _, sql := range []string{"rename user a", "rename user a to b,", "rename user a identified by 'p' to b", "rename user if exists a to b"} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err)
	}
}
