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
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type kase struct {
	a       string
	b       string
	want    string
	wantErr bool
}

func Test_MakeNameOfPartitionTable(t *testing.T) {
	kases := []kase{
		{partitionDelimiter, "abc", "", true},
		{"abc", partitionDelimiter, "", true},
		{"", "abc", "", true},
		{"abc", "", "", true},
		{"abc", "def", fmt.Sprintf("%sabc%sdef", partitionDelimiter, partitionDelimiter), false},
	}

	for _, k := range kases {
		r1, r11 := MakeNameOfPartitionTable(k.a, k.b)
		if k.wantErr {
			require.False(t, r1)
		} else {
			require.True(t, r1)
			require.Equal(t, k.want, r11)

			r2, a, b := SplitNameOfPartitionTable(r11)
			require.True(t, r2)
			require.Equal(t, a, k.a)
			require.Equal(t, b, k.b)
		}
	}
}

func Test_SplitNameOfPartitionTable(t *testing.T) {
	kases := []kase{
		{"", "", "abc", true},
		{"", "", partitionDelimiter + "abc", true},
		{"", "", partitionDelimiter, true},
		{"", "", partitionDelimiter + partitionDelimiter, true},
		{"", "", partitionDelimiter + "a" + partitionDelimiter, true},
		{"", "", partitionDelimiter + "" + partitionDelimiter + "b", true},
		{"a", "b", partitionDelimiter + "a" + partitionDelimiter + "b", false},
	}

	for _, k := range kases {
		r1, a, b := SplitNameOfPartitionTable(k.want)
		if k.wantErr {
			require.False(t, r1)
		} else {
			require.True(t, r1)
			require.Equal(t, a, k.a)
			require.Equal(t, b, k.b)
		}
	}
}
