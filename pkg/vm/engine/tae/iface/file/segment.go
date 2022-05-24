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

package file

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/layout/segment"
)

type SegmentFileFactory = func(dir string, id uint64) Segment

type Segment interface {
	Base
	OpenBlock(id uint64, colCnt int, indexCnt map[int]int) (Block, error)
	WriteTS(ts uint64) error
	ReadTS() uint64
	String() string
	RemoveBlock(id uint64)
	GetSegmentFile() *segment.Segment
	Replay(ids []uint64, colCnt int, indexCnt map[int]int, cache *bytes.Buffer) error
	// IsAppendable() bool
}
