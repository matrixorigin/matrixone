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

package db

import (
	"bytes"
	"errors"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
)

// Block is a high-level wrapper of the block type in memory. It
// only provides some essential interfaces used by computation layer.
type Block struct {
	// Segment this block belongs to
	Host  *Segment
	// uint64 representation of block id
	Id    uint64
	// string representation of block id
	StrId string
}

// Rows returns how many rows this block contains currently.
func (blk *Block) Rows() int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.GetRowCount())
}

// Size returns the memory usage of the certain column in a block.
func (blk *Block) Size(attr string) int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.Size(attr))
}

// ID returns the string representation of this block's id.
func (blk *Block) ID() string {
	return blk.StrId
}

// Prefetch prefetches the given columns of this block.
// We use the readahead system call, so if the data has
// been already fetched in page cache, nothing would happen.
func (blk *Block) Prefetch(attrs []string) {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	if data == nil {
		return
	}
	defer data.Unref()
	for _, attr := range attrs {
		if err := data.Prefetch(attr); err != nil {
			// TODO
			panic(err)
		}
	}
}

// Read reads the given columns into memory, and build a pipeline batch
// with the given reference count used by computation layer.
// For memory reuse, we pass two buffer array down and all the temp
// usage of memory would be taken in those buffers. (e.g. decompress)
func (blk *Block) Read(cs []uint64, attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error) {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	if data == nil {
		return nil, errors.New(fmt.Sprintf("Specified blk %d not found", blk.Id))
	}
	defer data.Unref()
	bat := batch.New(true, attrs)
	bat.Vecs = make([]*vector.Vector, len(attrs))
	for i, attr := range attrs {
		vec, err := data.GetVectorCopy(attr, compressed[i], deCompressed[i])
		if err != nil {
			return nil, err
		}
		vec.Ref = cs[i]
		bat.Vecs[i] = vec
	}
	return bat, nil
}

