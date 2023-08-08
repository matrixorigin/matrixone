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

package fileservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRCBytesPool(t *testing.T) {
	bs := RCBytesPool.Get(0)
	assert.Equal(t, rcBytesPoolMinCap, cap(bs.Value))
	bs.Release()

	bs = RCBytesPool.Get(rcBytesPoolMaxCap)
	assert.Equal(t, rcBytesPoolMaxCap, cap(bs.Value))
	bs.Release()

	bs = RCBytesPool.Get(rcBytesPoolMaxCap * 2)
	assert.Equal(t, rcBytesPoolMaxCap*2, len(bs.Value))
	bs.Release()
}
