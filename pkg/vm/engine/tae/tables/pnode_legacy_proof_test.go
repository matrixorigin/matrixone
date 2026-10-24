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

package tables

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestRelLogicalIDLegacyProofIsNarrow(t *testing.T) {
	moTables := catalog.NewEmptySchema(pkgcatalog.MO_TABLES)
	require.NoError(t, moTables.AppendCol("rel_id", types.T_uint64.ToType()))
	require.NoError(t, moTables.AppendCol(
		pkgcatalog.SystemRelAttr_LogicalID,
		types.T_uint64.ToType(),
	))
	logicalSeqnum := moTables.ColDefs[1].SeqNum

	ctx := withRelLogicalIDLegacyProof(context.Background(), moTables, "mo_tables-object")
	require.True(t, objectio.CanFillMissingColumn(ctx, "mo_tables-object", logicalSeqnum))
	require.False(t, objectio.CanFillMissingColumn(ctx, "other-object", logicalSeqnum))
	require.False(t, objectio.CanFillMissingColumn(ctx, "mo_tables-object", logicalSeqnum+1))

	userTable := catalog.NewEmptySchema("user_table")
	require.NoError(t, userTable.AppendCol(
		pkgcatalog.SystemRelAttr_LogicalID,
		types.T_uint64.ToType(),
	))
	ctx = withRelLogicalIDLegacyProof(context.Background(), userTable, "user-object")
	require.False(t, objectio.CanFillMissingColumn(ctx, "user-object", userTable.ColDefs[0].SeqNum))
}
