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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunk(t *testing.T) {
	data := []byte("hello, world")

	for limit := 1; limit <= len(data)+1; limit++ {
		chunks := Split(data, limit)

		merged := make([]byte, 0, len(data))
		merged = AppendChunk(merged, chunks...)

		require.True(t, bytes.Equal(data, merged))
	}
}

func TestSplit(t *testing.T) {
	buf := make([]byte, 11)
	chunks := Split(buf, 5)
	require.Equal(t, 3, len(chunks))
	require.Equal(t, 5, len(chunks[0]))
	require.Equal(t, 5, len(chunks[1]))
	require.Equal(t, 1, len(chunks[2]))
}
