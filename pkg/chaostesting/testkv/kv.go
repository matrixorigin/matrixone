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

package testkv

import (
	"sync"
	"sync/atomic"
)

type KV struct {
	kv     sync.Map
	sem    chan struct{}
	numOps int64
	logSet func(key, value any)
	logGet func(key, value any)
}

func NewKV(
	maxClients int,
) *KV {
	return &KV{
		sem: make(chan struct{}, maxClients),
	}
}

func (k *KV) Set(key any, value any) {
	defer func() {
		if k.logSet != nil {
			k.logSet(key, value)
		}
	}()
	k.sem <- struct{}{}
	defer func() {
		<-k.sem
	}()
	k.kv.Store(key, value)
	atomic.AddInt64(&k.numOps, 1)
}

func (k *KV) Get(key any) (value any) {
	defer func() {
		if k.logGet != nil {
			k.logGet(key, value)
		}
	}()
	k.sem <- struct{}{}
	defer func() {
		<-k.sem
	}()
	value, _ = k.kv.Load(key)
	atomic.AddInt64(&k.numOps, 1)
	return
}
