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

package lengthutf8

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountUTF8CodePoints(t *testing.T) {
	cases := map[string]uint64{
		"abc":   3,
		"":      0,
		"   ":   3,
		"中国123": 5,
		"abc😄":  4,
		"中国中国中国中国中国中国中国中国中国中国1234":      24,
		"中国中国中国中国中国中国中国中国中国中国1234😄ggg!": 29,
	}

	for input, expected := range cases {
		actual := CountUTF8CodePoints([]byte(input))
		require.Equal(t, expected, actual)
	}
}
