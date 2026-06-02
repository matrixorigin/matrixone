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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// newIntervalExpr builds `INTERVAL <n> <unit>` the same way the grammar does,
// so the test exercises the real TTL expression format path.
func newIntervalExpr(n int64, unit string) *FuncExpr {
	return &FuncExpr{
		FuncName: NewCStr("interval", 1),
		Exprs: Exprs{
			NewNumVal(n, strconv.FormatInt(n, 10), false, P_int64),
			NewTimeUnitExpr(unit),
		},
	}
}

func TestTableOptionTTLFormat(t *testing.T) {
	// Cover the core TTL syntax output: `col + INTERVAL n unit`.
	expr := NewBinaryExpr(PLUS, NewUnresolvedColName("created_at"), newIntervalExpr(7, "day"))
	ttl := NewTableOptionTTL(expr)
	ctx := NewFmtCtx(dialect.MYSQL)
	ttl.Format(ctx)
	require.Equal(t, "ttl = created_at + INTERVAL 7 day", ctx.String())
	require.Equal(t, "tree.TableOptionTTL", ttl.TypeName())
	ttl.Free()

	enable := NewTableOptionTTLEnable("ON")
	ctx = NewFmtCtx(dialect.MYSQL)
	enable.Format(ctx)
	require.Equal(t, "ttl_enable = 'ON'", ctx.String())
	require.Equal(t, "tree.TableOptionTTLEnable", enable.TypeName())
	enable.Free()

	// A value containing a single quote must be escaped so the output re-parses.
	enableQuoted := NewTableOptionTTLEnable("O'N")
	ctx = NewFmtCtx(dialect.MYSQL)
	enableQuoted.Format(ctx)
	require.Equal(t, "ttl_enable = 'O''N'", ctx.String())
	enableQuoted.Free()

	interval := NewTableOptionTTLJobInterval("1h")
	ctx = NewFmtCtx(dialect.MYSQL)
	interval.Format(ctx)
	require.Equal(t, "ttl_job_interval = '1h'", ctx.String())
	require.Equal(t, "tree.TableOptionTTLJobInterval", interval.TypeName())
	interval.Free()

	intervalQuoted := NewTableOptionTTLJobInterval("a'b")
	ctx = NewFmtCtx(dialect.MYSQL)
	intervalQuoted.Format(ctx)
	require.Equal(t, "ttl_job_interval = 'a''b'", ctx.String())
	intervalQuoted.Free()
}

func TestAlterTableRemoveTTLFormat(t *testing.T) {
	remove := NewAlterTableRemoveTTL()
	ctx := NewFmtCtx(dialect.MYSQL)
	remove.Format(ctx)
	require.Equal(t, "remove ttl", ctx.String())
	require.Equal(t, "tree.AlterTableRemoveTTL", remove.TypeName())
	remove.Free()
}

// ttlOptions allocates a fresh set of the three TTL table options.
func ttlOptions() []TableOption {
	return []TableOption{
		NewTableOptionTTL(NewBinaryExpr(PLUS, NewUnresolvedColName("created_at"), newIntervalExpr(7, "day"))),
		NewTableOptionTTLEnable("ON"),
		NewTableOptionTTLJobInterval("1h"),
	}
}

// TestCreateTableResetFreesTTLOptions covers the TTL cases in both
// CreateTable.reset switches (Options and DTOptions).
func TestCreateTableResetFreesTTLOptions(t *testing.T) {
	ct := &CreateTable{
		Options:   ttlOptions(),
		DTOptions: ttlOptions(),
	}
	require.NotPanics(t, func() { ct.reset() })
}

// TestAlterTableResetFreesTTLOptions covers the TTL / REMOVE TTL cases in
// AlterTable.reset.
func TestAlterTableResetFreesTTLOptions(t *testing.T) {
	opts := ttlOptions()
	at := &AlterTable{
		Options: []AlterTableOption{opts[0], opts[1], opts[2], NewAlterTableRemoveTTL()},
	}
	require.NotPanics(t, func() { at.reset() })
}

// TestPartitionResetFreesTTLOptions covers the TTL cases in the option-free
// switches of Partition.reset and SubPartition.reset (these share the generic
// TableOption Free handling).
func TestPartitionResetFreesTTLOptions(t *testing.T) {
	p := &Partition{Options: ttlOptions()}
	require.NotPanics(t, func() { p.reset() })

	sp := &SubPartition{Options: ttlOptions()}
	require.NotPanics(t, func() { sp.reset() })
}
