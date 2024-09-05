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

package memorycache

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

// _RC is an atomic reference counter
type _RC struct {
	val atomic.Int32

	mu   sync.Mutex
	logs []rcLog
}

type rcLog struct {
	msg        string
	stacktrace malloc.StacktraceID
	refs       int32
}

func (r *_RC) log(msg string) {
	r.mu.Lock()
	r.logs = append(r.logs, rcLog{
		msg:        msg,
		stacktrace: malloc.GetStacktraceID(1),
		refs:       r.val.Load(),
	})
	r.mu.Unlock()
}

func (r *_RC) init() {
	r.val.Store(1)
	r.log("init")
}

func (r *_RC) refs() int32 {
	return r.val.Load()
}

func (r *_RC) inc() {
	switch v := r.val.Add(1); {
	case v <= 1:
		r.dump(os.Stderr)
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	}
	r.log("inc")
}

func (r *_RC) dec() bool {
	defer r.log("dec")
	switch v := r.val.Add(-1); {
	case v < 0:
		r.dump(os.Stderr)
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	case v == 0:
		return true
	default:
		return false
	}
}

func (r *_RC) dump(w io.Writer) {
	r.mu.Lock()
	for i, log := range r.logs {
		fmt.Fprintf(w, "%d: msg %s, refs %d, stack %s\n",
			i, log.msg, log.refs, log.stacktrace.String(),
		)
	}
	r.mu.Unlock()
}
