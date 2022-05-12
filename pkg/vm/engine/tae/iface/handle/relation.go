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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Reader interface {
	Next(ctx interface{}, attrs []string) (*batch.Batch, error)
}

type Relation interface {
	io.Closer
	ID() uint64
	Rows() int64
	Size(attr string) int64
	String() string
	SimplePPString(common.PPLevel) string
	GetCardinality(attr string) int64
	Schema() interface{}
	MakeSegmentIt() SegmentIt
	MakeReader() Reader
	MakeBlockIt() BlockIt

	RangeDelete(id *common.ID, start, end uint32) error
	Update(id *common.ID, row uint32, col uint16, v interface{}) error
	GetByFilter(filter *Filter) (id *common.ID, offset uint32, err error)
	GetValue(id *common.ID, row uint32, col uint16) (interface{}, error)

	BatchDedup(col *vector.Vector) error
	Append(data *batch.Batch) error

	GetMeta() interface{}
	CreateSegment() (Segment, error)
	CreateNonAppendableSegment() (Segment, error)
	GetSegment(id uint64) (Segment, error)

	SoftDeleteSegment(id uint64) (err error)
}

type RelationIt interface {
	Iterator
	GetRelation() Relation
}
