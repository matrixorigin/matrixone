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

package disttae

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZonemapMarshalAndUnmarshal(t *testing.T) {
	var z Zonemap
	for i := 0; i < len(z); i++ {
		z[i] = byte(i)
	}

	data, err := z.Marshal()
	require.NoError(t, err)

	var ret Zonemap
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, z, ret)
}
