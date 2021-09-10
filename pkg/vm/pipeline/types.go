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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
)

const (
	PrefetchNum = 4
)

type Pipeline struct {
	cs    []uint64
	attrs []string
	ins   vm.Instructions
}

type block struct {
	siz int64
	bat *batch.Batch
	blk engine.Block
}

type queue struct {
	pi  int // prefetch index
	siz int64
	bs  []block
}
