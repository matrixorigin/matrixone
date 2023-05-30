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
	reqLabel := &RequestLabel{
		Labels: map[string]string{
			"account": "a1",
			"k1":      "v1",
			"k2":      "v2",
		},
	}
	data, err := reqLabel.Encode()
	require.NoError(t, err)

	l := &RequestLabel{}
	reader := bufio.NewReader(bytes.NewReader(data))
	err = l.Decode(reader)
	require.NoError(t, err)
	require.Equal(t, 3, len(l.Labels))
	require.Equal(t, "a1", l.Labels["account"])
	require.Equal(t, "v1", l.Labels["k1"])
	require.Equal(t, "v2", l.Labels["k2"])
}
