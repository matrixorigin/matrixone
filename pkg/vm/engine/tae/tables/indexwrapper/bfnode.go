// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BfReader struct {
	bfKey  string
	idx    uint16
	reader dataio.Reader
	typ    types.Type
}

func NewBfReader(
	id *common.ID,
	typ types.Type,
	metaloc string,
	fs *objectio.ObjectFS,
) *BfReader {
	reader, _ := blockio.NewObjectReader(fs.Service, metaloc)

	return &BfReader{
		idx:    id.Idx,
		bfKey:  metaloc,
		reader: reader,
		typ:    typ,
	}
}

func (r *BfReader) getBloomFilter() (index.StaticFilter, error) {
	_, _, extent, _, _ := blockio.DecodeLocation(r.bfKey)
	bf, err := r.reader.LoadBloomFilter(context.Background(), r.idx, []uint32{extent.Id()}, nil)
	if err != nil {
		// TODOa: Error Handling?
		return nil, err
	}
	return bf[0], err
}

func (r *BfReader) MayContainsKey(key any) (b bool, err error) {
	bf, err := r.getBloomFilter()
	if err != nil {
		return
	}
	return bf.MayContainsKey(key)
}

func (r *BfReader) MayContainsAnyKeys(keys containers.Vector) (b bool, m *roaring.Bitmap, err error) {
	bf, err := r.getBloomFilter()
	if err != nil {
		return
	}
	return bf.MayContainsAnyKeys(keys)
}

func (r *BfReader) Destroy() error { return nil }
