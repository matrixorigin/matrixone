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

// Register used in execution pipeline and shared with all operators of the same pipeline.
type Register struct {
	// Ss, temporarily stores the row number list in the execution of operators
	// and it can be reused in the future execution.
	Ss [][]int64
	// Ax, stores the result of the previous operator.
	Ax interface{}
	// Ts, temporarily stores the column data in the execution of operators
	// and it can be reused in the future execution to avoid mem alloc and type casting overhead.
	Ts []interface{}
	// Ws, receives result of multi previous operators from other pipelines
	// e.g. merge operator.
	Ws []*WaitRegister
}

//Limitation specifies the maximum resources that can be used in one query.
type Limitation struct {
	// Size, memory threshold.
	Size int64
	// BatchRows, max rows for batch.
	BatchRows int64
	// BatchSize, max size for batch.
	BatchSize int64
	// PartitionRows, max rows for partition.
	PartitionRows int64
}

// Process used in query execution,
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	// Id, query id.
	Id  string
	Reg Register
	Lim Limitation
	// Gm, records the resources used in current query.
	Gm *guest.Mmu
	// Mp, a pool for fast allocation and deallocation of objects.
	Mp *mempool.Mempool
	// Refer, traces the count of the reference of variables.
	Refer map[string]uint64
}
