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
	"sync/atomic"
)

func (d *Data) Bytes() []byte {
	return d.Buf()
}

// Buf returns the underlying buffer of the Data
func (d *Data) Buf() []byte {
	if d == nil {
		return nil
	}
	return d.buf
}

func (d *Data) Truncate(n int) *Data {
	d.buf = d.buf[:n]
	return d
}

func (d *Data) refs() int32 {
	return d.ref.refs()
}

func (d *Data) acquire() {
	d.ref.acquire()
}

func (d *Data) release(size *atomic.Int64) {
	if d != nil && d.ref.release() {
		d.free(size)
	}
}
