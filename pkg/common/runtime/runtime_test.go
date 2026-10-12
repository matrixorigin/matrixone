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
	"sync"
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
	assert.False(t, rt.CompareAndDeleteGlobalVariables(k, "other"))
	assert.True(t, rt.CompareAndDeleteGlobalVariables(k, v))
	_, ok = rt.GetGlobalVariables(k)
	assert.False(t, ok)
}

func TestServiceRuntimeRetirement(t *testing.T) {
	sid := t.Name()
	old, next := DefaultRuntime(), DefaultRuntime()
	defaultRuntime := ServiceRuntime("")
	t.Cleanup(func() {
		CompareAndDeleteServiceBasedRuntime(sid, old)
		CompareAndDeleteServiceBasedRuntime(sid, next)
	})
	assert.False(t, CompareAndDeleteServiceBasedRuntime(sid, old))
	SetupServiceBasedRuntime(sid, old)
	assert.False(t, CompareAndDeleteServiceBasedRuntime(sid, next))
	assert.Same(t, old, ServiceRuntime(sid))
	assert.True(t, CompareAndDeleteServiceBasedRuntime(sid, old))
	assert.Nil(t, ServiceRuntime(sid))
	assert.False(t, CompareAndDeleteServiceBasedRuntime(sid, old))

	SetupServiceBasedRuntime(sid, old)
	SetupServiceBasedRuntime(sid, next)
	assert.False(t, CompareAndDeleteServiceBasedRuntime(sid, old))
	assert.Same(t, next, ServiceRuntime(sid))
	assert.True(t, CompareAndDeleteServiceBasedRuntime(sid, next))

	// Whichever operation linearizes first, retiring old must preserve next.
	SetupServiceBasedRuntime(sid, old)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; SetupServiceBasedRuntime(sid, next) }()
	go func() { defer wg.Done(); <-start; CompareAndDeleteServiceBasedRuntime(sid, old) }()
	close(start)
	wg.Wait()
	assert.Same(t, next, ServiceRuntime(sid))
	assert.Same(t, defaultRuntime, ServiceRuntime(""))
}
