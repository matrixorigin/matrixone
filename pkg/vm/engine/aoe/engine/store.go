package engine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
)

func (s *store) SetBlocks(blocks []aoe.Block){
	s.blocks = blocks
}

func (s *store) GetBatch(refCount []uint64, attrs []string) *batData {
	if !s.start {
		s.mu.Lock()
		if s.start {
			s.mu.Unlock()
			goto GET
		}
		s.start = true
		s.mu.Unlock()
		s.ReadStart(refCount, attrs)
	}
GET:
	bat, ok := <-s.rhs
	if !ok {
		return nil
	}
	return bat
}

func (s *store) SetBatch(bat *batData){
	s.rhs <- bat
}

func (s *store) ReadStart(refCount []uint64, attrs []string) {
	num := 4
	mod := len(s.blocks) / num
	if mod == 0 {
		mod = 1
	}
	workers := make([]worker, 0)
	for i := 0; i < num; i++ {
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
	if len(workers) == 0 {
		s.SetBatch(nil)
		close(s.rhs)
		return
	}
	s.workers = len(workers)
	for j := 0; j < len(workers); j++{
		go workers[j].Start(refCount, attrs)
	}
}

func (s *store) RemoveWorker(id int32) {
	if s.workers < 1 {
		panic("workers error")
	}
	s.workers--
	if s.workers == 0 {
		s.SetBatch(nil)
		close(s.rhs)
	}
}