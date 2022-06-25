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

	"github.com/stretchr/testify/assert"
)

type O struct {
	BaseMvcc
}

func TestMVCC(t *testing.T) {
	mvcc0 := &O{}
	mvcc1 := &O{}
	mvcc2 := &O{}
	mvcc0.Unpin = func(i interface{}) {}
	mvcc1.Unpin = func(i interface{}) {}
	mvcc2.Unpin = func(i interface{}) {}
	mvcc0.Pin = func(i interface{}) {}
	mvcc1.Pin = func(i interface{}) {}
	mvcc2.Pin = func(i interface{}) {}
	mvcc0.GetObject = func() interface{} { return *mvcc0 }
	mvcc1.GetObject = func() interface{} { return *mvcc1 }
	mvcc2.GetObject = func() interface{} { return *mvcc2 }
	assert.NotNil(t, mvcc0.Object())
	mvcc0.SetNextVersion(mvcc1)
	mvcc1.SetNextVersion(mvcc2)
	mvcc1.SetPrevVersion(mvcc0)
	mvcc2.SetPrevVersion(mvcc1)
	assert.Nil(t, mvcc0.GetPrevVersion())
	assert.Nil(t, mvcc2.GetNextVersion())
	assert.NotNil(t, mvcc0.GetNextVersion())
	assert.NotNil(t, mvcc2.GetPrevVersion())
	mvcc2.OnVersionStale()
	mvcc1.OnVersionStale()
}
