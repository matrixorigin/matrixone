package engine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
)

func (s *store) SetBlocks(blocks []aoe.Block){
	s.blocks = blocks
}

func (s *store) GetBatch(refCount []uint64, attrs []string, id int32) *batData {
	if !s.start {
		s.mutex.Lock()
		if s.start {
			s.mutex.Unlock()
			goto GET
		}
		s.start = true
		s.mutex.Unlock()
		s.ReadStart(refCount, attrs)
	}
GET:
	bat, ok := <-s.rhs[id]
	if !ok {
		return nil
	}
	return bat
}

func (s *store) SetBatch(bat *batData, id int32){
	s.rhs[id] <- bat
}

func (s *store) CloseRHS(id int32){
	s.SetBatch(nil, id)
	close(s.rhs[id])

}

func (s *store) ReadStart(refCount []uint64, attrs []string) {
	if len(s.blocks) == 0 {
		for idx := range s.rhs {
			s.SetBatch(nil, int32(idx))
			close(s.rhs[idx])
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
				blocks: s.blocks[i*mod:],
				id:		int32(i),
				storeReader: s,
			}
			workers = append(workers, wk)
			break
		}
		wk := worker{
			blocks: s.blocks[i*mod : (i+1)*mod],
			id:		int32(i),
			storeReader: s,
		}
		workers = append(workers, wk)
	}
	i++
	if i < num {
		for j := i; j < num; j++ {
			s.SetBatch(nil, int32(j))
			close(s.rhs[j])
		}
	}
	for j := 0; j < len(workers); j++{
		workers[j].bufferCount = len(s.readers) / len(workers) * 2
		go workers[j].Start(refCount, attrs)
	}
}