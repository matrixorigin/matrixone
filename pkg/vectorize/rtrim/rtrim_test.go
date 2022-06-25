// Copyright 2022 Matrix Origin
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

package rtrim

import (
	"bytes"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCountSpacesFromRight(t *testing.T) {
	cases := map[string]int32{
		"":     0,
		" ":    1,
		"  ":   2,
		"   ":  3,
		"a ":   1,
		" a ":  1,
		" a  ": 2,
		"  a ": 1,
		" 你好 ": 1,
		"a　":   0, // fullwidth space
	}

	for input, expected := range cases {
		actual := CountSpacesFromRight(&types.Bytes{Data: []byte(input), Lengths: []uint32{uint32(len(input))}, Offsets: []uint32{0}})
		require.Equal(t, expected, actual, input)
	}

	multiStrings := []string{
		"",
		" ",
		"  ",
		"   ",
		" a",
		" a ",
		" a  ",
		"  a ",
		" 你好 ",
		"a　", // fullwidth space
	}
	multiStringCase := types.Bytes{}
	var offset uint32 = 0
	for _, input := range multiStrings {
		multiStringCase.Data = append(multiStringCase.Data, []byte(input)...)
		multiStringCase.Lengths = append(multiStringCase.Lengths, uint32(len(input)))
		multiStringCase.Offsets = append(multiStringCase.Offsets, offset)

		offset += uint32(len(input))
	}
	require.Equal(t, int32(11), CountSpacesFromRight(&multiStringCase))
}

func TestRtrim(t *testing.T) {
	multiStrings := []string{
		"",
		" ",
		"  ",
		"   ",
		" a",
		" a ",
		" a  ",
		"  a ",
		" 你好 ",
		"a　", // fullwidth space
	}
	multiStringCase := types.Bytes{}
	buf := bytes.Buffer{}
	var offset uint32 = 0
	trimed := 0
	for _, input := range multiStrings {
		buf.WriteString(input)
		trimed += len(input) - len(strings.TrimRight(input, " "))
		multiStringCase.Lengths = append(multiStringCase.Lengths, uint32(len(input)))
		multiStringCase.Offsets = append(multiStringCase.Offsets, offset)

		offset += uint32(len(input))
	}
	multiStringCase.Data = buf.Bytes()

	spacesCount := CountSpacesFromRight(&multiStringCase)
	require.Equal(t, spacesCount, int32(trimed))
	rs := types.Bytes{
		Data:    make([]byte, len(multiStringCase.Data)-int(spacesCount)),
		Lengths: make([]uint32, len(multiStringCase.Lengths)),
		Offsets: make([]uint32, len(multiStringCase.Offsets)),
	}

	rtrim(&multiStringCase, &rs)
	require.Equal(t, 10, len(rs.Lengths))
	require.Equal(t, 10, len(rs.Offsets))
	require.Equal(t, int(spacesCount), len(multiStringCase.Data)-len(rs.Data))
	for i := 0; i < len(multiStrings); i++ {
		r := string(rs.Get(int64(i)))
		want := strings.TrimRight(multiStrings[i], " ")
		require.Equal(t, r, want)
	}
}
