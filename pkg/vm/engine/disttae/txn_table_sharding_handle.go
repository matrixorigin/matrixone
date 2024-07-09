// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// HandleShardingReadRows handles sharding read rows
func HandleShardingReadRows(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	rows, err := tbl.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeUint64(rows), nil
}

func getTxnTable(
	ctx context.Context,
	param shard.ReadParam,
	engine engine.Engine,
) (*txnTable, error) {
	// TODO: reduce mem allocate
	proc, err := process.GetCodecService().Decode(
		ctx,
		param.Process,
	)
	if err != nil {
		return nil, err
	}

	db := &txnDatabase{
		op:           proc.GetTxnOperator(),
		databaseName: param.TxnTable.DatabaseName,
		databaseId:   param.TxnTable.DatabaseID,
	}

	item, err := db.getTableItem(
		ctx,
		uint32(param.TxnTable.AccountID),
		param.TxnTable.TableName,
		engine.(*Engine),
	)
	if err != nil {
		return nil, err
	}

	return newTxnTableWithItem(
		db,
		item,
		proc,
	), nil
}
