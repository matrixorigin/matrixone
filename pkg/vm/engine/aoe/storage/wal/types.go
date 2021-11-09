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

type ShardAwareWal interface {
	Wal
	InitShard(uint64, uint64) error
	GetShardCheckpointId(uint64) uint64
	GetShardCurrSeqNum(uint64) uint64
	GetAllPendingEntries() []*shard.ItemsToCheckpointStat
}

type ShardWal struct {
	Wal ShardAwareWal
	Id  uint64
}

func NewWalShard(shardId uint64, wal ShardAwareWal) *ShardWal {
	return &ShardWal{
		Wal: wal,
		Id:  shardId,
	}
}

func (wal *ShardWal) GetShardId() uint64 {
	return wal.Id
}

func (wal *ShardWal) Init(index uint64) error {
	return wal.Wal.InitShard(wal.Id, index)
}

func (wal *ShardWal) GetCheckpointId() uint64 {
	return wal.Wal.GetShardCheckpointId(wal.Id)
}

func (wal *ShardWal) GetCurrSeqNum() uint64 {
	return wal.Wal.GetShardCurrSeqNum(wal.Id)
}

func (wal *ShardWal) Log(payload Payload) (*Entry, error) {
	return wal.Wal.Log(payload)
}

func (wal *ShardWal) SyncLog(payload Payload) error {
	return wal.Wal.SyncLog(payload)
}

func (wal *ShardWal) Checkpoint(v interface{}) {
	wal.Wal.Checkpoint(v)
}
