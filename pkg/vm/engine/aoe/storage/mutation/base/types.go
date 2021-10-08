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

package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type BlockFlusher = func(base.INode, batch.IBatch, *metadata.Block, *dataio.TransientBlockFile) error

type MockSize struct {
	size uint64
}

func NewMockSize(size uint64) *MockSize {
	s := &MockSize{size: size}
	return s
}

func (mz *MockSize) Size() uint64 {
	return mz.size
}

type IMutableBlock interface {
	base.INode
	GetData() batch.IBatch
	GetFile() *dataio.TransientBlockFile
	GetSegmentedIndex() (uint64, bool)
	GetMeta() *metadata.Block
	SetStale()
	Flush() error
}
