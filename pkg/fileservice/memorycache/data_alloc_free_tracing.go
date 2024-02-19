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

// use race flag to enable data alloc/free tracing

//go:build race
// +build race

package memorycache

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func newData(n int, size *atomic.Int64) *Data {
	if n == 0 {
		return nil
	}
	size.Add(int64(n))
	b := malloc.Alloc(n)
	d := &Data{buf: b}
	d.ref.init(1)
	runtime.SetFinalizer(d, func(d *Data) {
		if d.buf != nil {
			logutil.Fatal(fmt.Sprintf("data %p is not freed: refs:%d\n%s",
				d, d.refs(), d.ref.dump()))
		}
	})
	return d
}

func (d *Data) free(size *atomic.Int64) {
	size.Add(-int64(cap(d.buf)))
	malloc.Free(d.buf)
	d.buf = nil
}
