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

package ivfflat

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// CaptureFlushOwnershipRanges is test-only glue for a storage-witness
// package that cannot import readutil into ivfflat (it would create an import
// cycle). Capture the actual INDEX_SEARCH_SCAN reader's storage request at
// Relation.Ranges, without implementing or overriding its ownership policy.
func CaptureFlushOwnershipRanges(t *testing.T, count, index int32, remote, stable bool) engine.RangesParam {
	t.Helper()
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	storage := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	proc.Base.TxnOperator = txn
	proc.Base.SessionInfo.StorageEngine = storage
	tableDef := &plan.TableDef{
		TblId:     1234,
		Name:      "entries",
		TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
	}
	storage.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "entries", proc).Return(relation, nil)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
	var request engine.RangesParam
	relation.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, param engine.RangesParam) (engine.RelData, error) {
			request = param
			return nil, nil
		})
	relation.EXPECT().BuildReaders(
		gomock.Any(), proc, nil, gomock.Nil(), 1, 0, false,
		gomock.Any(), gomock.Any()).Return(nil, nil)
	spec := &plan.IndexSearchScan{
		Index: &plan.IndexDef{IndexAlgo: "ivfflat"}, SourceTable: &plan.ObjectRef{},
	}
	if stable {
		// Use the wire field so the same test also runs against a baseline
		// that does not yet define stable_partition_ownership.
		wire := protowire.AppendTag(nil, 25, protowire.VarintType)
		require.NoError(t, spec.Unmarshal(protowire.AppendVarint(wire, 1)))
	}
	reader, err := NewPlanReader(proc, spec, searchplugin.Request{Identity: searchplugin.ScanIdentity{
		PartitionCount: count, PartitionIndex: index, IsRemote: remote,
	}})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	res, err := reader.(*planReader).scanner.ScanRelation(sqlexec.RelationScanRequest{
		Schema: "db", Table: "entries", Columns: []string{"pk"},
	})
	require.NoError(t, err)
	defer res.Close()
	require.NotNil(t, request.Rsp)
	require.Equal(t, count, request.Rsp.CNCNT)
	require.Equal(t, index, request.Rsp.CNIDX)
	require.Equal(t, count > 1, request.Rsp.ShuffleByObjectID)
	return request
}
