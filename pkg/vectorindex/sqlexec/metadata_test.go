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

package sqlexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestResolveVariableFunc(t *testing.T) {
	jstr := `{"cfg":{"kmeans_train_percent":{"t":"I", "v":10}, 
	"kmeans_max_iteration":{"t":"I", "v":4}, 
	"ivf_threads_build":{"t":"I", "v":23},
	"action":{"t":"S", "v":"action string"},
	"float":{"t":"F", "v":23.3}
	}, "action": "xxx"}`

	m, err := NewMetadataFromJson(jstr)
	require.Nil(t, err)

	f := m.ResolveVariableFunc

	v1, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, v1, any(int64(10)))

	v2, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, v2, any(int64(4)))

	v3, err := f("ivf_threads_build", false, false)
	require.Nil(t, err)
	require.Equal(t, v3, any(int64(23)))

	v4, err := f("float", false, false)
	require.Nil(t, err)
	require.Equal(t, v4, any(float64(23.3)))

	v5, err := f("action", false, false)
	require.Nil(t, err)
	require.Equal(t, v5, any("action string"))
}

func TestMetadataWriter(t *testing.T) {

	writer := NewMetadataWriter()
	writer.AddInt("kmeans_train_percent", 10)
	writer.AddInt("kmeans_max_iteration", 20)
	writer.AddString("string_param", "hello")
	writer.AddFloat("float_param", 44.56)

	js, err := writer.Marshal()
	require.Nil(t, err)

	fmt.Println(string(js))

	bj, err := bytejson.ParseFromString(string(js))
	require.Nil(t, err)

	bytes, err := bj.Marshal()
	require.Nil(t, err)

	m, err := NewMetadata(bytes)
	require.Nil(t, err)

	f := m.ResolveVariableFunc

	v1, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, v1, any(int64(10)))

	v2, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, v2, any(int64(20)))

	v4, err := f("float_param", false, false)
	require.Nil(t, err)
	require.Equal(t, v4, any(float64(44.56)))

	v5, err := f("string_param", false, false)
	require.Nil(t, err)
	require.Equal(t, v5, any("hello"))

}
