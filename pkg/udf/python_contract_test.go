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

package udf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPythonInlineArtifactDigestMatchesCrossLanguageVector(t *testing.T) {
	const want = "d3b06769662f0e30fca60e6dedc479c5aacef6c488c6489366fdb540ee374f26"
	require.Equal(t, want, PythonInlineArtifactDigest("add", "def add(ctx, value): return value"))
	require.True(t, IsSHA256Digest(want))
	require.False(t, IsSHA256Digest("D3B06769662F0E30FCA60E6DED C479C5AACEF6C488C6489366FDB540EE374F26"))
}

func TestPythonEnvironmentDigestIsCurrentAndStable(t *testing.T) {
	first, err := PythonEnvironmentDigest()
	require.NoError(t, err)
	second, err := PythonEnvironmentDigest()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.True(t, IsSHA256Digest(first))
}
