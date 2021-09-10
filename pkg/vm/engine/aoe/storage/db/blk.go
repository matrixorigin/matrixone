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

type Block struct {
	Host  *Segment
	Id    uint64
	StrId string
}

func (blk *Block) Rows() int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.GetRowCount())
}

func (blk *Block) Size(attr string) int64 {
	data := blk.Host.Data.StrongRefBlock(blk.Id)
	defer data.Unref()
	return int64(data.Size(attr))
}

func (blk *Block) ID() string {
	return blk.StrId
}

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

