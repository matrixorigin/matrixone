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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestLabel(t *testing.T) {
	salt := make([]byte, 20)
	for i := 0; i < 20; i++ {
		salt[i] = byte(i)
	}
	extraInfo := &ExtraInfo{
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
	data, err := extraInfo.Encode()
	require.NoError(t, err)

	e := &ExtraInfo{}
	reader := bufio.NewReader(bytes.NewReader(data))
	err = e.Decode(reader)
	require.NoError(t, err)
	require.Equal(t, 3, len(e.Label.Labels))
	require.Equal(t, "a1", e.Label.Labels["account"])
	require.Equal(t, "v1", e.Label.Labels["k1"])
	require.Equal(t, "v2", e.Label.Labels["k2"])
	require.Equal(t, true, e.InternalConn)
	require.Equal(t, salt, e.Salt)
}
