// Copyright 2022 Matrix Origin
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

package model

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
)

type LRUCache interface {
	Set(ctx context.Context, k any, v []byte)
	Get(ctx context.Context, k any) ([]byte, bool)
	Size() int64
}

type simpleLRU struct {
	impl lrucache.LRU[any, fileservice.Bytes]
}

func NewSimpleLRU(capacity int64) LRUCache {
	return &simpleLRU{
		impl: *lrucache.New[any, fileservice.Bytes](capacity, nil, nil),
	}
}

func (lru *simpleLRU) Size() int64 {
	return lru.impl.Used()
}

func (lru *simpleLRU) Get(ctx context.Context, k any) (v []byte, ok bool) {
	v, ok = lru.impl.Get(ctx, k, false)
	return
}

func (lru *simpleLRU) Set(ctx context.Context, k any, v []byte) {
	lru.impl.Set(ctx, k, v, false)
}
