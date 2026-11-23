// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPointSampleRequiresDisjointIntervals(t *testing.T) {
	rare := pointSampleInterval(0, 2)
	rareSibling := pointSampleInterval(28, 2)
	broad := pointSampleInterval(182, 2)
	require.Greater(t, rare.upper, 0.0)
	require.Equal(t, 1, separatedPointSampleWinner([]indexPointInterval{broad, rare}))
	require.Equal(t, 0, separatedPointSampleWinner([]indexPointInterval{rare, broad}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{rare, rareSibling}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{broad, rare, rareSibling}))
}
