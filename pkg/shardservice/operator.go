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

package shardservice

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
)

func newDeleteAllOp() pb.Operator {
	return pb.Operator{
		Type: pb.OpType_DeleteALL,
	}
}

func newDeleteOp(shard pb.TableShard) pb.Operator {
	return pb.Operator{
		Type:       pb.OpType_DeleteShard,
		TableShard: shard,
	}
}

func newCreateTableOp(tableID uint64) pb.Operator {
	return pb.Operator{
		Type:    pb.OpType_CreateTable,
		TableID: tableID,
	}

}

func newAddOp(shard pb.TableShard) pb.Operator {
	return pb.Operator{
		Type:       pb.OpType_AddShard,
		TableShard: shard,
	}
}
