// Copyright 2021 Matrix Origin
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

package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSampleIIIBasic(t *testing.T) {
	r := NewSampleIII[int32](time.Millisecond*5, 3, 6)
	for i := 0; i < 40; i++ {
		time.Sleep(5 * time.Millisecond)
		x := int32(500 * i)
		r.Append(x)
	}

	short, mid, long := r.QueryTrend()
	t.Logf("%+v, %v, %v, %v", r, short, mid, long)
	require.True(t, long > TrendStable, long)
	require.True(t, mid > TrendStable, mid)
	require.True(t, short > TrendStable, short)

	for i := 0; i < 60; i++ {
		time.Sleep(5 * time.Millisecond)
		r.Append(42)
	}

	short, mid, long = r.QueryTrend()
	t.Logf("%+v, %v, %v, %v", r, short, mid, long)
	require.Equal(t, TrendStable, short)
	require.Equal(t, TrendStable, mid)
	require.True(t, TrendIncI >= long)
	require.True(t, TrendDecI <= long)

	for i := 0; i < 60; i++ {
		time.Sleep(5 * time.Millisecond)
		r.Append(int32(-500 * i))
	}

	short, mid, long = r.QueryTrend()
	t.Logf("%+v, %v, %v, %v", r, short, mid, long)
	require.True(t, long < TrendStable, long)
	require.True(t, mid < TrendStable, mid)
	require.True(t, short < TrendStable, short)
}
