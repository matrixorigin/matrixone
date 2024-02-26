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

// use race flag to enable refcnt tracing

//go:build race
// +build race

package memorycache

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

// refcnt is an atomic reference counter
type refcnt struct {
	sync.Mutex
	val  atomic.Int32
	msgs []string
}

func (r *refcnt) init(val int32) {
	r.val.Store(val)
	r.trace("init")
}

func (r *refcnt) refs() int32 {
	return r.val.Load()
}

func (r *refcnt) acquire() {
	switch v := r.val.Add(1); {
	case v <= 1:
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	}
	r.trace("acquire")
}

func (r *refcnt) release() bool {
	v := r.val.Add(-1)
	if v < 0 {
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	}
	r.trace("release")
	return v == 0
}

func (r *refcnt) trace(msg string) {
	if !EnableTracing {
		return
	}
	traceMsg := fmt.Sprintf("%s: refs=%d\n%s", msg, r.refs(), string(debug.Stack()))
	r.Lock()
	r.msgs = append(r.msgs, traceMsg)
	r.Unlock()
}

func (r *refcnt) dump() string {
	r.Lock()
	defer r.Unlock()
	return strings.Join(r.msgs, "\n")
}
