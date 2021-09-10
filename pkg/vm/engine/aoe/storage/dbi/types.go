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

package dbi

import (
	"io"
)

type ISnapshot interface {
	io.Closer
	SegmentIds() []uint64
	NewIt() ISegmentIt
	GetSegment(id uint64) ISegment
}

type IBlock interface {
	GetID() uint64
	GetSegmentID() uint64
	GetTableID() uint64
	Prefetch() IBatchReader
}

type ISegment interface {
	NewIt() IBlockIt
	GetID() uint64
	GetTableID() uint64
	BlockIds() []uint64
	GetBlock(id uint64) IBlock
}

type Iterator interface {
	io.Closer
	Next()
	Valid() bool
}

type IBlockIt interface {
	Iterator
	GetHandle() IBlock
}

type ISegmentIt interface {
	Iterator
	GetHandle() ISegment
}
