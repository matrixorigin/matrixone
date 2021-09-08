package main

import (
	"fmt"
	"matrixone/pkg/logutil"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	tidStart := uint64(0)
	tidEnd := uint64(10)
	timeoutC := time.After(100 * time.Millisecond)
	func() {
		for {
			select {
			case <-timeoutC:
				logutil.Error("wait for available shard timeout")
				return
			default:
				if tidStart < tidEnd {
					id := tidStart
					atomic.AddUint64(&tidStart, 1)
					logutil.Infof("alloc table id finished, id is %d, endId is %d", id, tidEnd)
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	if tidEnd-tidStart < 20 {
		println(12345)
	}

	start := uint64(1)
	end := uint64(100)

	count := int32(0)

	threadNum := 5

	wg := sync.WaitGroup{}

	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			for {
				if start > end || end > 200 {
					wg.Done()
					fmt.Printf("Goroutine %d completed, start is %d, end is %d\n", Goid(), start, end)
					return
				} else {
					atomic.AddInt32(&count, 1)
					id := start
					atomic.AddUint64(&start, 1)
					fmt.Printf("Goroutine %d get id %d\n", Goid(), id)
					if end-start < 10 {
						fmt.Printf("start refresh range. %d\n", end)
						refresh(end)
						atomic.AddUint64(&end, 20)
					}
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Total amount of id dispatched is %d", count)
}

func Goid() int {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("panic recover:panic info:%v\n", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func refresh(num uint64) {
	fmt.Printf("call dispatch. %d\n", num)
	time.Sleep(100 * time.Millisecond)
}
