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

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestWaterliner(t *testing.T) {
	current := mockTimestamp(100, 100)

	w := NewWaterliner()

	/* ---- waterline not initialized ---- */
	waterline := w.Waterline()
	require.Equal(t, timestamp.Timestamp{}, waterline)

	/* ---- advance waterline ---- */
	w.Advance(current)
	waterline = w.Waterline()
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
