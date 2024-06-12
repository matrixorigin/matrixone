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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/require"
)

func TestData(t *testing.T) {
	allocator := malloc.NewDefault(nil)

	var size atomic.Int64
	d := newData(allocator, 1, &size)
	require.NotEqual(t, nil, d)
	// test refs
	require.Equal(t, int32(1), d.refs())
	// test Buf
	buf := d.Buf()
	buf[0] = 1
	require.Equal(t, byte(1), d.Buf()[0])
	// test Truncate
	d.Slice(0)
	require.Equal(t, 0, len(d.Buf()))
	// test acquire
	d.acquire()
	require.Equal(t, int32(2), d.refs())
	// test release
	d.Release()
	require.Equal(t, int32(1), d.refs())
	d.Release()
	require.Equal(t, int64(0), size.Load())
	// boundary test
	d = newData(allocator, 0, &size)
	require.Equal(t, (*Data)(nil), d)
}
