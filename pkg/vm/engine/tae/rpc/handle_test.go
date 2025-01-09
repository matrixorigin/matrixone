// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestHandleInspectPolicy(t *testing.T) {
	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	ctx := context.Background()
	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	_, err = testutil.CreateRelation2(ctx, asyncTxn, database, &catalog.Schema{
		Name:  "test1",
		Extra: &apipb.SchemaExtra{},
	})
	require.NoError(t, err)
	require.NoError(t, asyncTxn.Commit(context.Background()))

	resp := &cmd_util.InspectResp{}
	_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
		AccessInfo: cmd_util.AccessInfo{},
		Operation:  "policy",
	}, resp)
	require.NoError(t, err)
	require.Equal(t, "(*) maxMergeObjN: 16, maxOsizeObj: 128MB, minOsizeQualified: 90MB, offloadToCnSize: 80000MB, hints: [Auto]", resp.Message)

	_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
		AccessInfo: cmd_util.AccessInfo{},
		Operation:  "policy -t db1.test1 -r 0 -m 0",
	}, resp)
	require.NoError(t, err)
	require.Equal(t, "(1000-test1) maxMergeObjN: 0, maxOsizeObj: 128MB, minOsizeQualified: 0MB, offloadToCnSize: 80000MB, hints: [Auto]", resp.Message)

	_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
		AccessInfo: cmd_util.AccessInfo{},
		Operation:  "policy -t db1.test1 -s true",
	}, resp)
	require.NoError(t, err)
	require.Equal(t, "(1000-test1) maxMergeObjN: 16, maxOsizeObj: 128MB, minOsizeQualified: 90MB, offloadToCnSize: 80000MB, hints: [Auto]", resp.Message)

	_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
		AccessInfo: cmd_util.AccessInfo{},
		Operation:  "policy -t db1.test1 -s true",
	}, resp)
	require.NoError(t, err)
	require.Equal(t, "run err: internal error: test1 is already locked", resp.Message)

	_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
		AccessInfo: cmd_util.AccessInfo{},
		Operation:  "policy -t db1.test1",
	}, resp)
	require.NoError(t, err)
	require.Equal(t, "(1000-test1) maxMergeObjN: 16, maxOsizeObj: 128MB, minOsizeQualified: 90MB, offloadToCnSize: 80000MB, hints: [Auto]", resp.Message)
}

func TestHandlePrecommitWriteError(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	list := []*apipb.Entry{
		{
			EntryType: apipb.Entry_Insert,
			Bat:       &apipb.Batch{Vecs: []apipb.Vector{{Type: types.NewProtoType(types.T_char)}}},
		},
		{
			EntryType:  apipb.Entry_Insert,
			DatabaseId: 1,
			TableId:    3,
			Bat:        &apipb.Batch{Vecs: []apipb.Vector{{Type: types.NewProtoType(types.T_char)}}},
		},
	}

	err := h.HandlePreCommitWrite(context.Background(), txn.TxnMeta{}, &apipb.PrecommitWriteCmd{EntryList: list}, &apipb.TNStringResponse{})
	require.Error(t, err)
}
