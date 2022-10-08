// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"github.com/google/uuid"
	"sync/atomic"
)

var (
	GlobalSeqNum uint64 = 0
)

func NextGlobalSeqNum() uint64 {
	return atomic.AddUint64(&GlobalSeqNum, uint64(1))
}

func GetGlobalSeqNum() uint64 {
	return atomic.LoadUint64(&GlobalSeqNum)
}

type IdAlloctor struct {
	id uint64
}

func NewIdAlloctor(from uint64) *IdAlloctor {
	if from == 0 {
		panic("should not be 0")
	}

	return &IdAlloctor{
		id: from - 1,
	}
}

func (alloc *IdAlloctor) Alloc() uint64 {
	return atomic.AddUint64(&alloc.id, uint64(1))
}

func (alloc *IdAlloctor) Get() uint64 {
	return atomic.LoadUint64(&alloc.id)
}

func (alloc *IdAlloctor) SetStart(start uint64) {
	alloc.id = start
}

type TxnIDAllocator struct {
}

func NewTxnIDAllocator() *TxnIDAllocator {
	return &TxnIDAllocator{}
}

func (alloc *TxnIDAllocator) Alloc() []byte {
	ts := uuid.New()
	return ts[:]
}
