// Copyright 2025 Matrix Origin
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

func TestTableOptionTTLFormat(t *testing.T) {
	expr := NewBinaryExpr(PLUS, NewUnresolvedColName("created_at"), NewNumVal(int64(7), "7", false, P_int64))
	ttl := NewTableOptionTTL(expr)
	ctx := NewFmtCtx(dialect.MYSQL)
	ttl.Format(ctx)
	require.Equal(t, "ttl = created_at + 7", ctx.String())
	require.Equal(t, "tree.TableOptionTTL", ttl.TypeName())
	ttl.Free()

	enable := NewTableOptionTTLEnable("ON")
	ctx = NewFmtCtx(dialect.MYSQL)
	enable.Format(ctx)
	require.Equal(t, "ttl_enable = 'ON'", ctx.String())
	require.Equal(t, "tree.TableOptionTTLEnable", enable.TypeName())
	enable.Free()

	interval := NewTableOptionTTLJobInterval("1h")
	ctx = NewFmtCtx(dialect.MYSQL)
	interval.Format(ctx)
	require.Equal(t, "ttl_job_interval = '1h'", ctx.String())
	require.Equal(t, "tree.TableOptionTTLJobInterval", interval.TypeName())
	interval.Free()
}

func TestAlterTableRemoveTTLFormat(t *testing.T) {
	remove := NewAlterTableRemoveTTL()
	ctx := NewFmtCtx(dialect.MYSQL)
	remove.Format(ctx)
	require.Equal(t, "remove ttl", ctx.String())
	require.Equal(t, "tree.AlterTableRemoveTTL", remove.TypeName())
	remove.Free()
}
