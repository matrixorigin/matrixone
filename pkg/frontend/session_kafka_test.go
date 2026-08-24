// Copyright 2026 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSessionKafkaState: unset -> NULL semantics, then last-write-wins.
// Offset 0 is a VALID recorded id (distinct from unset).
func TestSessionKafkaState(t *testing.T) {
	ses := &Session{}
	_, ok := ses.LastKafkaMessageID()
	require.False(t, ok, "no scan yet: not found (the builtin renders NULL)")

	ses.SetLastKafkaMessageID(0)
	id, ok := ses.LastKafkaMessageID()
	require.True(t, ok, "offset 0 is a real recorded id")
	require.Equal(t, int64(0), id)

	ses.SetLastKafkaMessageID(4711)
	id, ok = ses.LastKafkaMessageID()
	require.True(t, ok)
	require.Equal(t, int64(4711), id)
}
