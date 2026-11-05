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

package tokenfilter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltinStopwordsRetriesAfterLoadFailure(t *testing.T) {
	resetBuiltinStopwords := func() {
		builtinStopMu.Lock()
		builtinStopSet = nil
		builtinStopMu.Unlock()
	}
	resetBuiltinStopwords()
	t.Cleanup(resetBuiltinStopwords)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "jieba.dict.utf8"), []byte("x 1 n\n"), 0o644))
	t.Setenv("MO_JIEBA_DICT_DIR", dir)

	_, err := BuiltinStopwords()
	require.Error(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, StopwordFile), []byte("the\nof\n"), 0o644))
	set, err := BuiltinStopwords()
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"the": {}, "of": {}}, set)
}
