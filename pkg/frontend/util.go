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

package frontend

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

type CloseFlag struct {
	//closed flag
	closed uint32
}

//1 for closed
//0 for others
func (cf *CloseFlag) setClosed(value uint32)  {
	atomic.StoreUint32(&cf.closed,value)
}

func (cf *CloseFlag) Open() {
	cf.setClosed(0)
}

func (cf *CloseFlag) Close() {
	cf.setClosed(1)
}

func (cf *CloseFlag) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) !=0
}

func (cf *CloseFlag) IsOpened() bool {
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


// GetRoutineId gets the routine id
func GetRoutineId() uint64 {
	data := make([]byte, 64)
	data = data[:runtime.Stack(data, false)]
	data = bytes.TrimPrefix(data, []byte("goroutine "))
	data = data[:bytes.IndexByte(data, ' ')]
	id, _ := strconv.ParseUint(string(data), 10, 64)
	return id
}

type DebugCounter struct {
	length  int
	counter []uint64
	Cf      CloseFlag
}

func NewDebugCounter(l int) *DebugCounter {
	return &DebugCounter{
		length:  l,
		counter: make([]uint64,l),
	}
}

func (dc *DebugCounter) Add(i int,v uint64)  {
	atomic.AddUint64(&dc.counter[i],v)
}

func (dc *DebugCounter) Set(i int, v uint64) {
	atomic.StoreUint64(&dc.counter[i],v)
}

func (dc *DebugCounter) Get(i int) uint64 {
	return atomic.LoadUint64(&dc.counter[i])
}

func (dc *DebugCounter) Len() int {
	return dc.length
}

func (dc *DebugCounter) DCRoutine() {
	dc.Cf.Open()

	for dc.Cf.IsOpened() {
		for i := 0; i < dc.length; i++ {
			if i != 0 && i % 8 == 0 {
				fmt.Printf("\n")
			}
			v := dc.Get(i)
			fmt.Printf("[%4d %4d]",i,v)
			dc.Set(i,0)
		}
		fmt.Printf("\n")
		time.Sleep(5 * time.Second)
	}
}

const (
	TIMEOUT_TYPE_SECOND int = iota
	TIMEOUT_TYPE_MILLISECOND
)

type Timeout struct {
	//last record of the time
	lastTime time.Time

	//period
	timeGap time.Duration

	//auto update
	autoUpdate bool
}

func NewTimeout(tg time.Duration, autoUpdateWhenChecked bool) *Timeout {
	return &Timeout{
		lastTime: time.Now(),
		timeGap:  tg,
		autoUpdate: autoUpdateWhenChecked,
	}
}

func (t *Timeout) UpdateTime(tn time.Time) {
	t.lastTime = tn
}

/*
----------+---------+------------------+--------
      lastTime     Now         lastTime + timeGap

return true  :  is timeout. the lastTime has been updated.
return false :  is not timeout. the lastTime has not been updated.
*/
func (t *Timeout) isTimeout() bool {
	if time.Since(t.lastTime) <= t.timeGap {
		return false
	}

	if t.autoUpdate {
		t.lastTime = time.Now()
	}

	return true
}

/*
length:
-1, complete string.
0, empty string
>0 , length of characters at the header of the string.
 */
func SubStringFromBegin(str string,length int) string {
	if length == 0 || length < -1{
		return ""
	}

	if length == -1 {
		return str
	}

	l := Min(len(str),length)
	if l != len(str) {
		return str[:l]+"..."
	}
	return str[:l]
}

/*
path exists in the system
return:
true/false - exists or not.
true/false - file or directory
error
 */
func PathExists(path string) (bool, bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return true, !fi.IsDir(),nil
	}
	if os.IsNotExist(err) {
		return false, false, err
	}

	return false, false, err
}