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

package base

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type IMemTable interface {
	common.IRef
	Append(bat *batch.Batch, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	Unpin()
	GetMeta() *md.Block
	GetID() common.ID
	String() string
}

type ICollection interface {
	common.IRef
	Append(bat *batch.Batch, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
	String() string
}

type ILimiter interface {
	ActiveCnt() uint32
	ActiveSize() uint64
	ApplySizeQuota(uint64) bool
	ApplyCntQuota(uint32) bool
}

type IManager interface {
	WeakRefCollection(id uint64) ICollection
	StrongRefCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
	GetLimiter() ILimiter
	String() string
}
