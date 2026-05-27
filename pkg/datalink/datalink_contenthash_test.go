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

package datalink

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A pinned datalink resolves to the immutable CAS object path, never the live path.
func TestParseDatalinkContentHashResolvesToCAS(t *testing.T) {
	hash := strings.Repeat("a", 64)
	moUrl, offsetSize, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash, nil)
	require.NoError(t, err)
	require.Equal(t, CASKey(hash), moUrl)
	require.NotEqual(t, "/a/b/c/1.txt", moUrl) // must not fall back to the live path
	require.Equal(t, []int64{0, -1}, offsetSize)
}

// An uppercase hash is normalized to lowercase so it matches the stored sha256 hex.
func TestParseDatalinkContentHashNormalizesCase(t *testing.T) {
	moUrl, _, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+strings.Repeat("A", 64), nil)
	require.NoError(t, err)
	require.Equal(t, CASKey(strings.Repeat("a", 64)), moUrl)
}

// offset/size still apply on top of a pinned object.
func TestParseDatalinkContentHashWithOffsetSize(t *testing.T) {
	hash := strings.Repeat("a", 64)
	moUrl, offsetSize, err := ParseDatalink("file:///a/b/c/1.txt?contenthash="+hash+"&offset=1&size=2", nil)
	require.NoError(t, err)
	require.Equal(t, CASKey(hash), moUrl)
	require.Equal(t, []int64{1, 2}, offsetSize)
}

// A pinned stage datalink does not need the live stage resolution (nil proc is fine).
func TestParseDatalinkContentHashStageNoLiveResolution(t *testing.T) {
	hash := strings.Repeat("b", 64)
	moUrl, _, err := ParseDatalink("stage://st/doc.txt?contenthash="+hash, nil)
	require.NoError(t, err)
	require.Equal(t, CASKey(hash), moUrl)
}

func TestParseDatalinkInvalidContentHash(t *testing.T) {
	cases := []string{
		"file:///a/b/c/1.txt?contenthash=abc",                        // too short
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("a", 63), // 63 chars
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("a", 65), // 65 chars
		"file:///a/b/c/1.txt?contenthash=" + strings.Repeat("z", 64), // non-hex
		"file:///a/b/c/1.txt?contenthash=",                           // empty
	}
	for _, c := range cases {
		_, _, err := ParseDatalink(c, nil)
		require.Error(t, err, c)
	}
}
