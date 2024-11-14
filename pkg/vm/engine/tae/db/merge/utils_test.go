// Copyright 2024 Matrix Origin
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

package merge

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResourceController(t *testing.T) {
	rc := new(resourceController)
	rc.setMemLimit(10000)
	require.Equal(t, int64(7500), rc.limit)
	require.Equal(t, int64(7500), rc.availableMem())

	rc.refresh()
	rc.limit = rc.using + 1
	require.Equal(t, int64(1), rc.availableMem())

	require.Panics(t, func() { rc.setMemLimit(0) })
}
