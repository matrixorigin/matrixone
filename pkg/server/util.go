package server

import "sync/atomic"

//a convenient data structure for closing
type CloseFlag struct {
	//closed flag
	closed uint32
}

//1 for closed
//0 for others
func (cf CloseFlag) setClosed(value uint32)  {
	atomic.StoreUint32(&cf.closed,value)
}

func (cf CloseFlag) Open() {
	cf.setClosed(0)
}

func (cf CloseFlag) Close() {
	cf.setClosed(1)
}

func (cf CloseFlag) isClosed() bool {
	return atomic.LoadUint32(&cf.closed) !=0
}

func (cf CloseFlag) isOpened() bool {
	return atomic.LoadUint32(&cf.closed) == 0
}

func min(a int, b int) int{
	if a < b {
		return a
	}else{
		return b
	}
}

func max(a int, b int) int{
	if a < b {
		return b
	}else{
		return a
	}
}