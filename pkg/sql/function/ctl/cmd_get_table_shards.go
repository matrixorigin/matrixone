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

package ctl

import (
	"fmt"
	"strings"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// select mo_ctl('cn', 'get-table-shards', 'table id')
func handleGetTableShards(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	tableID, err := format.ParseStringUint64(parameter)
	if err != nil {
		return Result{}, err
	}

	s := shardservice.GetService(proc.GetService())
	_, shards, err := s.GetTableShards(tableID)
	if err != nil {
		return Result{}, err
	}

	var info strings.Builder
	for _, shard := range shards {
		info.WriteString(fmt.Sprintf("shard %d, replicas %d: [", shard.ShardID, len(shard.Replicas)))
		n := len(shard.Replicas)
		for i, replica := range shard.Replicas {
			info.WriteString(fmt.Sprintf("%s:%s", replica.CN, replica.State.String()))
			if i < n-1 {
				info.WriteString(", ")
			}
		}
		info.WriteString("]\n")
	}

	return Result{
		Method: GetTableShards,
		Data:   info.String(),
	}, nil
}
