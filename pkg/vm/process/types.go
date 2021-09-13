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

package process

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"sync"
)

/*
type Process interface {
	Size() int64
	HostSize() int64

	Free([]byte)
	Alloc(int64) ([]byte, error)
}
*/

// WaitRegister channel
type WaitRegister struct {
	Wg *sync.WaitGroup
	Ch chan interface{}
}

/*
{
	// normal
	bat = proc.Reg.Ax // 获取上一个算子的结果
	rbat = process(bat)
	proc.Reg.Ax = rba // 把结果喂给下一个算子

	// merge
	bat = proc.Reg.Ax // 获取上一个算子的结果
	proc.Reg.Ws[i].Ch <- bat
}
*/
type Register struct {
	Ss [][]int64       // 临时数组，存放行号列表，为了内存复用
	Ax interface{}     // 存放上一个算子产生的结果
	Ts []interface{}   // 缓存列数据/向量
	Ws []*WaitRegister // merge算子
}

type Limitation struct {
	Size          int64 // memory threshold
	BatchRows     int64 // max rows for batch
	BatchSize     int64 // max size for batch
	PartitionRows int64 // max rows for partition
}

type Process struct {
	Id    string // query id
	Reg   Register
	Lim   Limitation // resource limits
	Gm    *guest.Mmu
	Mp    *mempool.Mempool // a pool for fast allocation and deallocation of objects.
	Refer map[string]uint64
}

//一个query多个pipeline， 一个pipeline只有一个proc
