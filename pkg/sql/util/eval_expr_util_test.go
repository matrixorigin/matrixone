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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHexToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = HexToInt("0x1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), val)

	val, err = HexToInt("0x123456789abcdef")
	require.NoError(t, err)
	require.Equal(t, uint64(81985529216486895), val)

	_, err = HexToInt("0xg")
	require.Error(t, err)
}

func TestBinaryToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = BinaryToInt("0x1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), val)

	val, err = BinaryToInt("0b0101010101001")
	require.NoError(t, err)
	require.Equal(t, uint64(2729), val)

	_, err = BinaryToInt("0x2")
	require.Error(t, err)
}

func TestScoreBinaryToInt(t *testing.T) {
	var val uint64
	var err error

	val, err = ScoreBinaryToInt("1")
	require.NoError(t, err)
	require.Equal(t, uint64(49), val)

	val, err = ScoreBinaryToInt("1234")
	require.NoError(t, err)
	require.Equal(t, uint64(825373492), val)

	_, err = ScoreBinaryToInt("123456789")
	require.Error(t, err)

	val, err = ScoreBinaryToInt("阿斯")
	require.NoError(t, err)
	require.Equal(t, uint64(256842263860911), val)
}
