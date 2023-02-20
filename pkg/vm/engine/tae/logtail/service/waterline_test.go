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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/require"
)

func TestWaterliner(t *testing.T) {
	current := mockTimestamp(100, 100)
	clock := mockPermanentClock(current)

	w := NewWaterliner(clock)

	/* ---- advance on uninitialized instance ---- */
	require.Panics(t, func() {
		w.Advance(current)
	})

	/* ---- initialized on its first call ---- */
	waterline := w.Waterline()
	require.Equal(t, current, waterline)

	/* ---- advance with a backward ts ---- */
	require.Panics(t, func() {
		prev := current.Prev()
		w.Advance(prev)
	})

	/* ---- advance with a monotonous ts ---- */
	next := current.Next()
	w.Advance(next)
	waterline = w.Waterline()
	require.Equal(t, next, waterline)
}

type permanentClock struct {
	ts timestamp.Timestamp
}

func mockPermanentClock(ts timestamp.Timestamp) clock.Clock {
	return &permanentClock{
		ts: ts,
	}
}

func (m *permanentClock) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	return m.ts, timestamp.Timestamp{}
}

func (m *permanentClock) Update(ts timestamp.Timestamp) {
	m.ts = ts
}

func (m *permanentClock) HasNetworkLatency() bool {
	return false
}

func (m *permanentClock) MaxOffset() time.Duration {
	return 0
}

func (m *permanentClock) SetNodeID(id uint16) {

}
