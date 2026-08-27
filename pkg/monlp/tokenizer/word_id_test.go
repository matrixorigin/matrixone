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

package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWordID(t *testing.T) {
	// A common dictionary word resolves to a stable non-negative id.
	id, ok, err := WordID("中国")
	require.NoError(t, err)
	require.True(t, ok, "expected a common jieba-dict word to resolve")
	require.GreaterOrEqual(t, id, int32(0))
	require.Less(t, id, int32(DictWordIDLimit))

	// The same word resolves to the same id (map is stable / loaded once).
	id2, ok2, err := WordID("中国")
	require.NoError(t, err)
	require.True(t, ok2)
	require.Equal(t, id, id2)

	// An out-of-dictionary token returns ok=false with no error (the caller maps
	// these to per-index overflow ids).
	_, ok, err = WordID("zzz_definitely_not_a_dict_word_qwerty")
	require.NoError(t, err)
	require.False(t, ok)
}
