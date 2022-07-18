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

package hakeeper

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type IDAllocator interface {
	// Next returns a new ID that can be used as the replica ID of a DN shard or
	// Log shard. When the return boolean value is false, it means no more ID
	// can be allocated at this time.
	Next() (uint64, bool)
	Set(first uint64, last uint64)
	Capacity() uint64
}

// Checker is the interface suppose to be implemented by HAKeeper's
// coordinator. Checker is supposed to be stateless - Checker is free to
// maintain whatever internal states, but those states should never be
// assumed to be persistent across reboots.
type Checker interface {
	// Check is periodically called by the HAKeeper for checking the cluster
	// health status, a list of Operator instances will be returned describing
	// actions required to ensure the high availability of the cluster.
	Check(alloc util.IDAllocator, cluster pb.ClusterInfo, dnState pb.DNState, logState pb.LogState, currentTick uint64) []pb.ScheduleCommand
}

// BootstrapManager is the interface suppose to be implemented by HAKeeper's
// bootstrap manager.
type BootstrapManager interface {
	Bootstrap(util.IDAllocator, pb.DNState, pb.LogState) ([]pb.ScheduleCommand, error)

	CheckBootstrap(pb.LogState) bool
}
