// Copyright 2022 Matrix Origin
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

package engine

import "github.com/matrixorigin/matrixone/pkg/vm/engine"

const (
	FileterNone = iota
	FileterEq
	FileterNe
	FileterLt
	FileterLe
	FileterGt
	FileterGe
	FileterBtw
)

func NewAoeSparseFilter(s *store, reader *aoeReader) *AoeSparseFilter {
	return &AoeSparseFilter{reader: reader, storeReader: s}
}

func (a AoeSparseFilter) Eq(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterEq,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Ne(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterNe,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Lt(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterLt,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Le(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterLe,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Gt(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterGt,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Ge(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterGe,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Btw(s string, i interface{}, i2 interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterBtw,
		param1: i,
		param2: i2,
	})
	return a.reader, nil
}

/*func (f *AoeSparseFilter) Eq(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Eq(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Ne(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Ne(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Lt(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Lt(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Le(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Le(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Gt(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Gt(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Ge(s string, i interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Ge(s, i)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}

func (f *AoeSparseFilter) Btw(s string, i interface{}, i2 interface{}) (engine.Reader, error) {
	blocks := make([]aoe.Block, 0)
	for _, sid := range f.storeReader.rel.segments {
		segment := f.storeReader.rel.Segment(sid)
		ids, _ := segment.NewSparseFilter().Btw(s, i, i2)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	f.storeReader.SetBlocks(blocks)
	return f.reader, nil
}*/
