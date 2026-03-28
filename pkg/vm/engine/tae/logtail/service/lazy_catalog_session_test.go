// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLazyCatalogFilterState_BeginActivation(t *testing.T) {
	s := &lazyCatalogFilterState{}

	ok := s.beginActivation(10, 1)
	assert.False(t, ok)

	s.configure([]uint32{0})
	assert.True(t, s.enabled)

	ok = s.beginActivation(10, 1)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), s.activatingSeqByAccount[10])
}

func TestLazyCatalogFilterState_CompleteActivation(t *testing.T) {
	s := &lazyCatalogFilterState{}
	s.configure([]uint32{0})

	s.beginActivation(10, 1)

	ok := s.completeActivation(10, 1)
	assert.True(t, ok)

	_, inActivating := s.activatingSeqByAccount[10]
	assert.False(t, inActivating)

	_, inActive := s.activeAccounts[10]
	assert.True(t, inActive)
}

func TestLazyCatalogFilterState_CompleteActivation_StaleSeq(t *testing.T) {
	s := &lazyCatalogFilterState{}
	s.configure([]uint32{0})

	s.beginActivation(10, 1)
	s.beginActivation(10, 2)

	ok := s.completeActivation(10, 1)
	assert.False(t, ok)

	ok = s.completeActivation(10, 2)
	assert.True(t, ok)
}

func TestLazyCatalogFilterState_ConfigureIdempotent(t *testing.T) {
	s := &lazyCatalogFilterState{}
	s.configure([]uint32{0})
	s.configure([]uint32{5})

	_, ok := s.activeAccounts[0]
	assert.True(t, ok)
	_, ok = s.activeAccounts[5]
	assert.True(t, ok)
}

func TestLazyCatalogFilterState_RefreshActiveAccountsSnapshot(t *testing.T) {
	s := &lazyCatalogFilterState{}
	s.configure([]uint32{0, 5, 10})

	snapshot := s.activeAccountsSnapshot.Load()
	if assert.NotNil(t, snapshot) {
		assert.True(t, snapshot.contains(0))
		assert.True(t, snapshot.contains(5))
		assert.True(t, snapshot.contains(10))
	}

	s.beginActivation(20, 1)
	assert.True(t, s.completeActivation(20, 1))

	snapshot = s.activeAccountsSnapshot.Load()
	if assert.NotNil(t, snapshot) {
		assert.True(t, snapshot.contains(20))
	}
}

func TestLazyCatalogFilterState_AbortActivation(t *testing.T) {
	s := &lazyCatalogFilterState{}
	s.configure([]uint32{0})
	s.beginActivation(10, 1)

	assert.True(t, s.abortActivation(10, 1))
	_, ok := s.activatingSeqByAccount[10]
	assert.False(t, ok)

	s.beginActivation(10, 2)
	assert.False(t, s.abortActivation(10, 1))
	assert.True(t, s.abortActivation(10, 2))
}
