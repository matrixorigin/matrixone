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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheASTOwnership(t *testing.T) {
	option := NewTableOptionAutoIDCache(1000000)
	ctx := NewFmtCtx(dialect.MYSQL)
	option.Format(ctx)
	require.Equal(t, "auto_id_cache = 1000000", ctx.String())
	require.Equal(t, "tree.TableOptionAutoIDCache", option.TypeName())
	option.Free()
	// These owners share the table-option grammar, including rejected ALTER
	// statements. Every reset must release the option without a missing-Free panic.
	owners := []func(*TableOptionAutoIDCache){
		func(o *TableOptionAutoIDCache) {
			n := &CreateTable{Options: []TableOption{o}}
			n.reset()
			require.Empty(t, n.Options)
		},
		func(o *TableOptionAutoIDCache) {
			n := &AlterTable{Options: AlterTableOptions{o}}
			n.reset()
			require.Empty(t, n.Options)
		},
		func(o *TableOptionAutoIDCache) {
			n := &Partition{Options: []TableOption{o}}
			n.reset()
			require.Empty(t, n.Options)
		},
		func(o *TableOptionAutoIDCache) {
			n := &SubPartition{Options: []TableOption{o}}
			n.reset()
			require.Empty(t, n.Options)
		},
	}
	for _, reset := range owners {
		reset(NewTableOptionAutoIDCache(1))
	}
	zero := NewTableOptionAutoIDCache(0)
	defer zero.Free()
	require.Zero(t, zero.Value)
}
