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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type MutableTable interface {
	common.IRef
	Append(bat *batch.Batch, index *shard.SliceIndex) (err error)
	Flush() error
	String() string
	GetMeta() *metadata.Table
}

type IManager interface {
	WeakRefTable(id uint64) MutableTable
	StrongRefTable(id uint64) MutableTable
	RegisterTable(interface{}) (c MutableTable, err error)
	UnregisterTable(id uint64) (c MutableTable, err error)
	TableIds() map[uint64]uint64
	String() string
}
