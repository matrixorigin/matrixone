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
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
)

func newDeleteAllOp() operator {
	return operator{
		createAt: time.Now(),
		cmd: pb.Cmd{
			Type: pb.CmdType_DeleteALL,
		},
	}
}

func newDeleteOp(shard pb.TableShard) operator {
	return operator{
		createAt: time.Now(),
		cmd: pb.Cmd{
			Type:       pb.CmdType_DeleteShard,
			TableShard: shard,
		},
	}
}

func newAddOp(shard pb.TableShard) operator {
	return operator{
		createAt: time.Now(),
		cmd: pb.Cmd{
			Type:       pb.CmdType_AddShard,
			TableShard: shard,
		},
	}
}
