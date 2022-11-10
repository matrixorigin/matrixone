// Copyright 2021 Matrix Origin
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

package common

import (
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestRefs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	helper := RefHelper{
		OnZeroCB: func() {

		},
	}
	assert.Equal(t, helper.RefCount(), int64(0))
	helper.Ref()
	assert.Equal(t, helper.RefCount(), int64(1))
	helper.Ref()
	helper.Unref()
	assert.Equal(t, helper.RefCount(), int64(1))
	helper.Unref()
	assert.Equal(t, helper.RefCount(), int64(0))
	assert.Panics(t, helper.Unref)
}
