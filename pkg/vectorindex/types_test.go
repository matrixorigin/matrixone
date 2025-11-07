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

package vectorindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCdc(t *testing.T) {
	key := int64(0)
	v := []float32{0, 1, 2}
	key2 := int64(1)
	v2 := []float32{1, 2, 3}

	cdc := NewVectorIndexCdc[float32](8192)

	// Insert
	cdc.Insert(key, v)

	js, err := cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]}]}`)

	// delete
	cdc.Delete(key)
	js, err = cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]},{"t":"D","pk":0}]}`)

	// upsert
	cdc.Upsert(key2, v2)

	js, err = cdc.ToJson()
	require.Nil(t, err)

	require.Equal(t, js, `{"cdc":[{"t":"I","pk":0,"v":[0,1,2]},{"t":"D","pk":0},{"t":"U","pk":1,"v":[1,2,3]}]}`)

	require.False(t, cdc.Empty())

	require.False(t, cdc.Full())

	cdc.Reset()

	js, err = cdc.ToJson()
	require.NoError(t, err)
	require.Equal(t, js, `{"cdc":[]}`)
}
