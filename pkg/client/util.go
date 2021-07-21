package client

import (
	"bytes"
	"runtime"
	"strconv"
	"sync/atomic"
)

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

func (cf CloseFlag) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) !=0
}

func (cf CloseFlag) IsOpened() bool {
	return atomic.LoadUint32(&cf.closed) == 0
}

func Min(a int, b int) int{
	if a < b {
		return a
	}else{
		return b
	}
}

func MinInt64(a int64, b int64) int64{
	if a < b {
		return a
	}else{
		return b
	}
}

func Max(a int, b int) int{
	if a < b {
		return b
	}else{
		return a
	}
}

func MaxInt64(a int64, b int64) int64{
	if a < b {
		return b
	}else{
		return a
	}
}

//get the outine id
func GetRoutineId() uint64 {
	data := make([]byte, 64)
	data = data[:runtime.Stack(data, false)]
	data = bytes.TrimPrefix(data, []byte("goroutine "))
	data = data[:bytes.IndexByte(data, ' ')]
	id, _ := strconv.ParseUint(string(data), 10, 64)
	return id
}