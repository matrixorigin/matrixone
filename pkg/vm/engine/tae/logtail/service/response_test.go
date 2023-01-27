// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseSize(t *testing.T) {
	maxMessageSize := 1024
	pool := NewLogtailServerSegmentPool(maxMessageSize)

	/* --- Fetch a segment --- */
	prev := pool.Acquire()
	t.Log("segment size:", prev.ProtoSize())
	require.True(t, prev.ProtoSize() > maxMessageSize)

	/* --- Release it --- */
	pool.Release(prev)

	/* --- Fetch again --- */
	curr := pool.Acquire()
	t.Log("segment size:", curr.ProtoSize())
	require.Equal(t, prev.ProtoSize(), curr.ProtoSize())

	curr.StreamID = math.MaxUint64
	curr.Sequence = math.MaxInt32
	curr.MaxSequence = math.MaxInt32
	curr.MessageSize = math.MaxInt32
	t.Log("max proto size:", curr.ProtoSize())

	limit := 2*maxMessageSize - curr.ProtoSize()
	t.Log("limited size:", limit)
	curr.Payload = curr.Payload[:limit]
	t.Log("final segment size:", curr.ProtoSize())
	require.Equal(t, curr.ProtoSize(), maxMessageSize)
}
