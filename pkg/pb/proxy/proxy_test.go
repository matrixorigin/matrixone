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
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCodec(t *testing.T) {
	v1 := Version{Minor: 1, Major: 2, Patch: 3}
	data, err := v1.Encode()
	require.NoError(t, err)

	var v2 Version
	err = v2.Decode(data)
	require.NoError(t, err)

	require.Equal(t, v1, v2)
}

func TestVersionAvailable(t *testing.T) {
	require.True(t, Version0.available())
	v := Version{}
	require.False(t, v.available())
	v = Version{Major: 1, Minor: 1, Patch: 0}
	require.False(t, v.available())
}

func TestVersionCompare(t *testing.T) {
	v1 := Version{Major: 1, Minor: 0, Patch: 0}
	require.True(t, v1.GE(v1))
	v2 := Version{Major: 1, Minor: 0, Patch: 2}
	require.True(t, v2.GE(v1))
	v0 := Version{Major: 0, Minor: 2, Patch: 0}
	require.False(t, v0.GE(v1))
}

func TestVersionCompatible(t *testing.T) {
	v1 := Version{Major: 1, Minor: 0, Patch: 0}
	v2 := Version{Major: 1, Minor: 0, Patch: 2}
	require.False(t, v1.compatible(v2))
	require.True(t, v2.compatible(v1))
}

func TestExtraInfoV0Codec(t *testing.T) {
	salt := make([]byte, 20)
	for i := 0; i < 20; i++ {
		salt[i] = byte(i)
	}
	extraInfo := &ExtraInfoV0{
		Salt:         salt,
		InternalConn: true,
		Label: RequestLabel{
			Labels: map[string]string{
				"account": "a1",
				"k1":      "v1",
				"k2":      "v2",
			},
		},
	}
	data1, err := extraInfo.Marshal(Version0)
	require.NoError(t, err)

	e := &ExtraInfoV0{}
	reader := bufio.NewReader(bytes.NewReader(data1))
	data2, err := readData(reader)
	require.NoError(t, err)
	buf := bytes.NewBuffer(data2[2:])
	var v Version
	require.NoError(t, binary.Read(buf, binary.LittleEndian, &v))
	err = e.Unmarshal(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 3, len(e.Label.Labels))
	require.Equal(t, "a1", e.Label.Labels["account"])
	require.Equal(t, "v1", e.Label.Labels["k1"])
	require.Equal(t, "v2", e.Label.Labels["k2"])
	require.Equal(t, true, e.InternalConn)
	require.Equal(t, salt, e.Salt)
}

func TestVersionedExtraInfoCodec(t *testing.T) {
	salt := make([]byte, 20)
	for i := 0; i < 20; i++ {
		salt[i] = byte(i)
	}
	i0 := &ExtraInfoV0{
		Salt:         salt,
		InternalConn: true,
		ConnectionID: 123,
		Label: RequestLabel{
			Labels: map[string]string{
				"account": "a1",
				"k1":      "v1",
				"k2":      "v2",
			},
		},
	}
	ve1 := NewVersionedExtraInfo(Version0, i0)
	data, err := ve1.Encode()
	require.NoError(t, err)

	ve2 := NewVersionedExtraInfo(Version0, nil)
	reader := bufio.NewReader(bytes.NewReader(data))
	err = ve2.Decode(reader)
	require.NoError(t, err)
	label, ok := ve2.ExtraInfo.GetLabel()
	require.True(t, ok)
	require.Equal(t, 3, len(label.Labels))
	require.Equal(t, "a1", label.Labels["account"])
	require.Equal(t, "v1", label.Labels["k1"])
	require.Equal(t, "v2", label.Labels["k2"])
	internalConn, ok := ve2.ExtraInfo.GetInternalConn()
	require.True(t, ok)
	require.Equal(t, true, internalConn)
	connID, ok := ve2.ExtraInfo.GetConnectionID()
	require.True(t, ok)
	require.Equal(t, 123, int(connID))
	salt_, ok := ve2.ExtraInfo.GetSalt()
	require.True(t, ok)
	require.Equal(t, salt, salt_)
}
