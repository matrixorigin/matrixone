// Copyright 2021 Matrix Origin
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

package wal

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
)

type Role uint8

const (
	HolderRole Role = iota
	BrokerRole
)

type Payload interface {
	Marshal() ([]byte, error)
}

type Wal interface {
	io.Closer
	Log(Payload) (*Entry, error)
	SyncLog(Payload) error
	Checkpoint(interface{})
	String() string
	GetRole() Role
}

type ShardWal interface {
	Wal
	InitShard(uint64, uint64) error
	GetShardCheckpointId(uint64) uint64
	GetShardCurrSeqNum(uint64) uint64
	GetAllPendingEntries() []*shard.ItemsToCheckpointStat
}
