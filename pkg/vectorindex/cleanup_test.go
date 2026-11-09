// Copyright 2026 Matrix Origin
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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type cleanupResource struct {
	calls      int
	err        error
	panicValue any
}

func (r *cleanupResource) Destroy() error {
	r.calls++
	if r.panicValue != nil {
		panic(r.panicValue)
	}
	return r.err
}

func TestDestroyIndexResourcesContinuesAfterFailure(t *testing.T) {
	resources := []*cleanupResource{
		{panicValue: "first"},
		{err: errors.New("destroy failed")},
		{panicValue: "second"},
		{},
	}
	require.Equal(t, "first", DestroyIndexResources("index", resources))
	for _, resource := range resources {
		require.Equal(t, 1, resource.calls)
	}
}
