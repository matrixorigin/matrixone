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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsIndexAsync(t *testing.T) {

	var json string
	json = "{}"
	ok, err := IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = `{"async": "true"}`
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, true)

	json = `{"async": 1}`
	_, err = IsIndexAsync(json)
	require.NotNil(t, err)
}
