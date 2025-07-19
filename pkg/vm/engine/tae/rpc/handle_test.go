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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

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
