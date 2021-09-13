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

package mock

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// ops "aoe/pkg/metadata3/ops"
)

func NewChunk(capacity uint64, meta *md.Block) *Chunk {
	c := &Chunk{
		Capacity: capacity,
		Count:    0,
	}
	return c
}

type Chunk struct {
	Capacity uint64
	Count    uint64
}

func (c *Chunk) Append(o *Chunk, offset uint64) (n uint64, err error) {
	max := c.Capacity - c.Count
	o_max := o.Count - offset
	if max >= o_max {
		n = o_max
		c.Count += o_max
	} else {
		n = max
		c.Count += max
	}
	return n, err
}

func (c *Chunk) GetCount() uint64 {
	return c.Count
}

type DataWriter interface {
	Write(obj interface{}) error
}

func NewDataWriter() DataWriter {
	w := &MockDataWriter{}
	return w
}

type MockDataWriter struct {
}

func (w *MockDataWriter) Write(obj interface{}) error {
	return nil
}
