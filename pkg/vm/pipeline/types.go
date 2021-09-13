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

package pipeline

import (
	"bytes"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
)

const (
	PrefetchNum           = 4
	CompressedBlockSize   = 1024
	UncompressedBlockSize = 4096
)

type Pipeline struct {
	cs    []uint64        // reference count for attribute
	attrs []string        // attribute list
	cds   []*bytes.Buffer // buffers for compressed data
	dds   []*bytes.Buffer // buffers for decompressed data
	ins   vm.Instructions // orders to be executed
}

type block struct {
	blk engine.Block
}

type queue struct {
	pi  int   // prefetch index
	siz int64 // not used now
	bs  []block
}
