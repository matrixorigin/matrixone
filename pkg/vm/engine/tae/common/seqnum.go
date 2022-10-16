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
	"sync/atomic"

	"github.com/google/uuid"
)

var (
	GlobalSeqNum atomic.Uint64
)

func NextGlobalSeqNum() uint64 {
	return GlobalSeqNum.Add(1)
}

func GetGlobalSeqNum() uint64 {
	return GlobalSeqNum.Load()
}

type IdAlloctor struct {
	id atomic.Uint64
}

func NewIdAlloctor(from uint64) *IdAlloctor {
	if from == 0 {
		panic("should not be 0")
	}

	alloc := &IdAlloctor{}
	alloc.id.Store(from - 1)
	return alloc
}

func (alloc *IdAlloctor) Alloc() uint64 {
	return alloc.id.Add(1)
}

func (alloc *IdAlloctor) Get() uint64 {
	return alloc.id.Load()
}

func (alloc *IdAlloctor) SetStart(start uint64) {
	alloc.id.Store(start)
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
