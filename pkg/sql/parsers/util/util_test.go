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

package util

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemovePrefixComment(t *testing.T) {
	require.Equal(t, "abcd", RemovePrefixComment("abcd"))
	require.Equal(t, "abcd", RemovePrefixComment("/*11111*/abcd"))
	require.Equal(t, "abcd", RemovePrefixComment("/**/abcd"))
	require.Equal(t, "/*/abcd", RemovePrefixComment("/*/abcd"))
	require.Equal(t, "/*abcd", RemovePrefixComment("/*abcd"))
	require.Equal(t, "*/abcd", RemovePrefixComment("*/abcd"))
}

func TestSplitBySemicolon(t *testing.T) {
	t1 := SplitBySemicolon("abc;abcd;abcde;")
	require.Equal(t, 3, len(t1))
	require.Equal(t, "abc", t1[0])
	require.Equal(t, "abcd", t1[1])
	require.Equal(t, "abcde", t1[2])

	t2 := SplitBySemicolon("abc/*123;*/;abcd;abcde;")
	require.Equal(t, 3, len(t2))
	require.Equal(t, "abc/*123;*/", t2[0])
	require.Equal(t, "abcd", t2[1])
	require.Equal(t, "abcde", t2[2])

	t3 := SplitBySemicolon("abc'123;';abcd;abcde;")
	require.Equal(t, 3, len(t3))
	require.Equal(t, "abc'123;'", t3[0])
	require.Equal(t, "abcd", t3[1])
	require.Equal(t, "abcde", t3[2])

	t4 := SplitBySemicolon("abc\"123;\";abcd;abcde;")
	require.Equal(t, 3, len(t4))
	require.Equal(t, "abc\"123;\"", t4[0])
	require.Equal(t, "abcd", t4[1])
	require.Equal(t, "abcde", t4[2])

	t5 := SplitBySemicolon("abc;abcd;abcde")
	require.Equal(t, 3, len(t5))
	require.Equal(t, "abc", t5[0])
	require.Equal(t, "abcd", t5[1])
	require.Equal(t, "abcde", t5[2])
}
