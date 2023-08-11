// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

const (
	comQuery = 3
)

func makeSimplePacket(payload string) []byte {
	l := 1 + len(payload)
	data := make([]byte, l+4)
	data[4] = comQuery
	copy(data[5:], payload)
	data[0] = byte(l)
	data[1] = byte(l >> 8)
	data[2] = byte(l >> 16)
	data[3] = 0
	return data
}

func packetLen(data []byte) (int32, error) {
	if len(data) < 3 {
		return 0, moerr.NewInternalErrorNoCtx("invalid data")
	}
	return int32(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16), nil
}

func TestConvert(t *testing.T) {
	p1 := &frontend.Packet{
		Length:     10,
		SequenceID: 0,
		Payload:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
	}
	b1 := packetToBytes(p1)
	p2 := bytesToPacket(b1)
	require.Equal(t, p1, p2)

	b2 := []byte{3, 0, 0, 1, 1, 2, 3}
	p3 := bytesToPacket(b2)
	b3 := packetToBytes(p3)
	require.Equal(t, b2, b3)
}

func TestPickTunnels(t *testing.T) {
	ts := make(tunnelSet)
	res := pickTunnels(ts, 3)
	require.Equal(t, len(res), 0)

	t1 := &tunnel{}
	ts.add(t1)
	res = pickTunnels(ts, 3)
	require.Equal(t, len(res), 1)

	t2 := &tunnel{}
	ts.add(t2)
	t3 := &tunnel{}
	ts.add(t3)
	res = pickTunnels(ts, 2)
	require.Equal(t, len(res), 2)
}

func TestSortSlice(t *testing.T) {
	var sorted = []any{"a", "b", "c", "d"}
	var s1 = []any{"c", "b", "a", "d"}
	var s2 = []any{"b", "a", "d", "c"}
	newS1 := sortSlice(s1)
	newS2 := sortSlice(s2)
	for i := 0; i < len(s1); i++ {
		require.Equal(t, sorted[i], newS1[i])
		require.Equal(t, sorted[i], newS2[i])
	}
}

func TestRawHash(t *testing.T) {
	label := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	require.Equal(t, 32, len(rawHash(label)))
}

func TestIsCmdQuery(t *testing.T) {
	var data []byte
	ret := isCmdQuery(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 2, 0}
	ret = isCmdQuery(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 3, 0}
	ret = isCmdQuery(data)
	require.True(t, ret)
}

func TestIsOKPacket(t *testing.T) {
	var data []byte
	ret := isOKPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 2, 0}
	ret = isOKPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 0, 0}
	ret = isOKPacket(data)
	require.True(t, ret)
}

func TestIsEOFPacket(t *testing.T) {
	var data []byte
	ret := isEOFPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 2, 0}
	ret = isEOFPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 0xFE, 0}
	ret = isEOFPacket(data)
	require.True(t, ret)
}

func TestIsErrPacket(t *testing.T) {
	var data []byte
	ret := isErrPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 2, 0}
	ret = isErrPacket(data)
	require.False(t, ret)

	data = []byte{0, 0, 0, 0, 0xFF, 0}
	ret = isErrPacket(data)
	require.True(t, ret)
}

func TestParseLabel(t *testing.T) {
	cases := []struct {
		str string
		ret map[string]string
	}{
		{
			str: "",
			ret: nil,
		},
		{
			str: "a=1",
			ret: map[string]string{"a": "1"},
		},
		{
			str: "a=1,",
			ret: map[string]string{"a": "1"},
		},
		{
			str: "a=1,b=2",
			ret: map[string]string{"b": "2", "a": "1"},
		},
		{
			str: "a=1,b",
			ret: map[string]string{"a": "1"},
		},
		{
			str: "a=1,b=",
			ret: map[string]string{"a": "1"},
		},
		{
			str: "a=1,=2",
			ret: map[string]string{"a": "1"},
		},
		{
			str: "a=1,=",
			ret: map[string]string{"a": "1"},
		},
	}

	for _, item := range cases {
		require.True(t, reflect.DeepEqual(item.ret, parseLabel(item.str)))
	}
}
