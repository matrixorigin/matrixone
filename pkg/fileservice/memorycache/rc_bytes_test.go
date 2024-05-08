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

	"github.com/stretchr/testify/require"
)

func TestRCBytes(t *testing.T) {
	var size atomic.Int64

	r := RCBytes{
		d:    newData(1, &size),
		size: &size,
	}
	// test Bytes
	r.Bytes()[0] = 1
	require.Equal(t, r.Bytes()[0], byte(1))
	// test Slice
	r = r.Slice(0).(RCBytes)
	require.Equal(t, 0, len(r.Bytes()))
	// test release
	r.Release()
	require.Equal(t, int64(0), size.Load())
}
