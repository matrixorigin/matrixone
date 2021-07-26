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

func MinUint64(a uint64, b uint64) uint64{
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

func MaxUint64(a uint64, b uint64) uint64{
	if a < b {
		return b
	}else{
		return a
	}
}

type Uint64List []uint64

func (ul Uint64List) Len() int {
	return len(ul)
}

func (ul Uint64List) Less(i,j int) bool {
	return ul[i] < ul[j]
}

func (ul Uint64List) Swap(i,j int) {
	ul[i],ul[j] = ul[j],ul[i]
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