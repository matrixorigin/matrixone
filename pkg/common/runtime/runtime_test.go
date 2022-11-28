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

package runtime

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestRuntime(t *testing.T) {
	rt := NewRuntime(metadata.ServiceType_CN, "cn0", nil)
	assert.Equal(t, metadata.ServiceType_CN, rt.ServiceType())
	assert.Equal(t, "cn0", rt.ServiceUUID())
	assert.NotNil(t, rt.Logger())

	k := "k"
	v := "v"
	rt.SetGlobalVariables(k, v)
	vv, ok := rt.GetGlobalVariables(k)
	assert.Equal(t, v, vv)
	assert.True(t, ok)
}
