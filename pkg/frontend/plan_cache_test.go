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

package frontend

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func hashString(sql string) string {
	hash := sha256.New()
	hash.Write([]byte(sql))
	return hex.EncodeToString(hash.Sum(nil))
}

func Test_BasicGet(t *testing.T) {
	pc := newPlanCache(5)

	pc.cache("abc", nil, nil)
	require.True(t, pc.isCached("abc"))
	require.Equal(t, pc.get("abc").sql, hashString("abc"))

	pc.cache("abcd", nil, nil)
	require.True(t, pc.isCached("abcd"))
	require.Equal(t, pc.get("abcd").sql, hashString("abcd"))

	require.False(t, pc.isCached("abcde"))
}

func Test_LRU(t *testing.T) {
	pc := newPlanCache(3)

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	require.True(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))

	pc.cache("4", nil, nil)
	require.True(t, pc.isCached("4"))
	require.False(t, pc.isCached("1"))

	require.Equal(t, pc.get("2").sql, hashString("2"))
	pc.cache("5", nil, nil)
	require.True(t, pc.isCached("5"))
	require.True(t, pc.isCached("4"))
	require.True(t, pc.isCached("2"))
	require.False(t, pc.isCached("3"))
}

func Test_CleanCache(t *testing.T) {
	pc := newPlanCache(3)

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	pc.cache("4", nil, nil)
	require.False(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))
	require.True(t, pc.isCached("4"))

	pc.clean()

	require.Nil(t, pc.get("1"))
	require.Nil(t, pc.get("2"))
	require.Nil(t, pc.get("3"))
	require.Nil(t, pc.get("4"))
	require.False(t, pc.isCached("1"))
	require.False(t, pc.isCached("2"))
	require.False(t, pc.isCached("3"))
	require.False(t, pc.isCached("3"))

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	pc.cache("4", nil, nil)
	require.False(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))
	require.True(t, pc.isCached("4"))
	require.Nil(t, pc.get("1"))
	require.NotNil(t, pc.get("2"))
	require.NotNil(t, pc.get("3"))
	require.NotNil(t, pc.get("4"))
}
