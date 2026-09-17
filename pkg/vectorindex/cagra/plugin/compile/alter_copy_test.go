// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// AlterCopyInitSQL must rebuild cagra from source on a COPY ALTER: cloneUnaffectedIndexes SKIPS
// the clone (SkipWholeIndex), so the replacement hidden tables start empty, and CagraSync only
// appends tag=1 event chunks -- the tag=0 sub-index is written ONLY by the build. Without an
// explicit REINDEX the replacement index has no base at all: the ts=0 replay puts the whole table
// in the CDC tail, every query brute-forces it, and a table large enough makes that overflow
// refuse admission. #29011
func TestAlterCopyInitSQL(t *testing.T) {
	ctx := newHandleCtx(true)

	startFromNow, initSQL, err := (Hooks{}).AlterCopyInitSQL(ctx, cagraIndexDefs())
	require.NoError(t, err)
	// startFromNow MUST be true. With a non-empty InitSQL the REINDEX FORCE_SYNC builds the base
	// from source and the tail arms at the post-build watermark; false would persist an empty
	// startTs, so the first normal iteration would CollectChanges from ts=0 and replay every
	// copied row into the tail on top of the freshly built base.
	require.True(t, startFromNow, "false would arm the tail from ts=0 -> full-table replay on top of the base")
	require.Equal(t, "ALTER TABLE `db1`.`t` ALTER REINDEX `ix` cagra FORCE_SYNC", initSQL)

	// Contrast: RestoreInitSQL rebuilds too, but from the block-cloned rows. It builds the
	// same statement and must escape identifiers the same way -- a restore whose InitSQL is
	// malformed can never execute, so the restored index stays base-less forever.
	_, restoreSQL, err := (Hooks{}).RestoreInitSQL(ctx, cagraIndexDefs())
	require.NoError(t, err)
	require.Equal(t, "ALTER TABLE `db1`.`t` ALTER REINDEX `ix` cagra FORCE_SYNC", restoreSQL)

	// Embedded backticks in db/table/index names are escaped (doubled) via the shared identifier
	// helper, so the post-commit REINDEX is valid SQL rather than malformed.
	btctx := newHandleCtx(true)
	btctx.stubCompileContext.qryDatabase = "d`b"
	btctx.stubCompileContext.originalTableDef = &plan.TableDef{
		Name: "s`rc",
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
	btdefs := map[string]*plan.IndexDef{catalog.Cagra_TblType_Metadata: {IndexName: "id`x"}}
	_, btSQL, err := (Hooks{}).AlterCopyInitSQL(btctx, btdefs)
	require.NoError(t, err)
	require.Equal(t, "ALTER TABLE `d``b`.`s``rc` ALTER REINDEX `id``x` cagra FORCE_SYNC", btSQL)

	_, btRestoreSQL, err := (Hooks{}).RestoreInitSQL(btctx, btdefs)
	require.NoError(t, err)
	require.Equal(t, "ALTER TABLE `d``b`.`s``rc` ALTER REINDEX `id``x` cagra FORCE_SYNC", btRestoreSQL)

	// Fail closed when the metadata def is absent: a silent (false, "") would leave the
	// replacement index base-less with no way to notice.
	_, _, err = (Hooks{}).AlterCopyInitSQL(ctx, map[string]*plan.IndexDef{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cagra_meta")
}
