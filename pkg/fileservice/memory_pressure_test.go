// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMemoryTight(t *testing.T) {
	// total == 0: always false regardless of used
	require.False(t, isMemoryTight(0, 0))
	require.False(t, isMemoryTight(0, 1000))

	// used <= 75%: not tight
	require.False(t, isMemoryTight(100, 0))
	require.False(t, isMemoryTight(100, 75))

	// used > 75%: tight
	require.True(t, isMemoryTight(100, 76))
	require.True(t, isMemoryTight(1000, 900))
}

func TestMemoryTight(t *testing.T) {
	// Just verify the function runs without panic on a real system.
	_ = memoryTight()
}
