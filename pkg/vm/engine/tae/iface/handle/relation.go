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

package handle

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Relation interface {
	io.Closer
	ID() uint64
	Rows() int64
	String() string
	SimplePPString(common.PPLevel) string
	GetCardinality(attr string) int64
	Schema() any
	MakeSegmentIt() SegmentIt
	MakeBlockIt() BlockIt

	DeleteByPhyAddrKey(key any) error
	GetValueByPhyAddrKey(key any, col int) (any, error)

	DeleteByPhyAddrKeys(keys containers.Vector) error

	RangeDelete(id *common.ID, start, end uint32, dt DeleteType) error
	Update(id *common.ID, row uint32, col uint16, v any) error
	GetByFilter(filter *Filter) (id *common.ID, offset uint32, err error)
	GetValue(id *common.ID, row uint32, col uint16) (any, error)
	GetValueByFilter(filter *Filter, col int) (any, error)
	UpdateByFilter(filter *Filter, col uint16, v any) error
	DeleteByFilter(filter *Filter) error

	BatchDedup(col containers.Vector) error
	Append(data *containers.Batch) error

	GetMeta() any
	CreateSegment(bool) (Segment, error)
	CreateNonAppendableSegment() (Segment, error)
	GetSegment(id uint64) (Segment, error)

	SoftDeleteSegment(id uint64) (err error)

	GetDB() (Database, error)
}

type RelationIt interface {
	Iterator
	GetRelation() Relation
}
