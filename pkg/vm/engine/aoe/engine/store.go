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

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
)

func (s *store) SetBlocks(blocks []aoe.Block) {
	s.blocks = blocks
}

func (s *store) GetBatch(refCount []uint64, attrs []string, reader *aoeReader) *batData {
	if !s.start {
		s.mutex.Lock()
		if s.start {
			s.mutex.Unlock()
			goto GET
		}
		s.start = true
		s.mutex.Unlock()
		for _, filter := range reader.filter{
			s.sparseFilter(&filter)
		}
		s.ReadStart(refCount, attrs)
	}
GET:
	bat, ok := <-s.rhs[reader.workerid]
	if !ok {
		return nil
	}
	return bat
}

func (s *store) SetBatch(bat *batData, id int32) {
	s.rhs[id] <- bat
}

func (s *store) GetBuffer(id int32) *batData {
	bat, ok := <-s.chs[id]
	if !ok {
		return nil
	}
	return bat
}

func (s *store) PutBuffer(bat *batData, id int32) {
	s.chs[id] <- bat
}

func (s *store) CloseRhs(id int32) {
	close(s.rhs[id])
}

func (s *store) CloseChs(id int32) {
	close(s.chs[id])
}

func (s *store) ReadStart(refCount []uint64, attrs []string) {
	if len(s.blocks) == 0 {
		for idx := range s.rhs {
			s.SetBatch(nil, int32(idx))
			s.CloseRhs(int32(idx))
			s.CloseChs(int32(idx))
		}
		return
	}
	num := s.iodepth
	mod := len(s.blocks) / num
	if mod == 0 {
		mod = 1
	}
	workers := make([]worker, 0)
	var i int
	for i = 0; i < num; i++ {
		if i == num-1 || i == len(s.blocks)-1 {
			wk := worker{
				blocks:      s.blocks[i*mod:],
				id:          int32(i),
				storeReader: s,
			}
			workers = append(workers, wk)
			break
		}
		wk := worker{
			blocks:      s.blocks[i*mod : (i+1)*mod],
			id:          int32(i),
			storeReader: s,
		}
		workers = append(workers, wk)
	}
	i++
	if i < num {
		for j := i; j < num; j++ {
			s.SetBatch(nil, int32(j))
			s.CloseRhs(int32(j))
			s.CloseChs(int32(j))
		}
	}
	for j := 0; j < len(workers); j++ {
		workers[j].bufferCount = len(s.readers) / s.iodepth * int(s.rel.cfg.ReaderBufferCount)
		go workers[j].Start(refCount, attrs)
	}
}

func (s *store) sparseFilter(filter *filterContext)  {
	switch filter.filterType {
	case FileterEq:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Eq(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterNe:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Ne(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterLt:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Lt(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterLe:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Le(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterGt:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Gt(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterGe:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Ge(filter.attr, filter.param1)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	case FileterBtw:
		blocks := make([]aoe.Block, 0)
		for _, sid := range s.rel.segments {
			segment := s.rel.Segment(sid)
			ids, _ := segment.NewSparseFilter().Btw(filter.attr, filter.param1, filter.param2)
			for _, id := range ids {
				if  blockExist(s.blocks, id){
					blocks = append(blocks, segment.Block(id))
				}
			}
		}
		s.SetBlocks(blocks)
		break
	default:
		panic("No Support")
	}
}

func blockExist(blocks []aoe.Block, iter string) bool {
	exist := false
	for _, block := range blocks {
		if block.ID() == iter {
			exist = true
		}
	}
	return exist
}