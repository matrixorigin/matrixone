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

package taskservice

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCronServiceState(t *testing.T) {
	s := &cronServiceState{}
	// start
	assert.True(t, s.canStart())
	// cannot start when started
	assert.False(t, s.canStart())
	// check inner state
	assert.True(t, s.started)
	assert.False(t, s.stopping)
	// stop
	assert.True(t, s.canStop())
	// cannot stop when stopping
	assert.False(t, s.canStop())
	// check inner state
	assert.True(t, s.started)
	assert.True(t, s.stopping)
	// cannot start during stop
	assert.False(t, s.canStart())
	// end stop
	s.endStop()
	// check inner state
	assert.False(t, s.started)
	assert.False(t, s.stopping)

	// cannot stop when stopped
	assert.False(t, s.canStop())
	// can start after stop
	assert.True(t, s.canStart())
}

func TestCronJobState(t *testing.T) {
	s := &cronJobState{}

	// can run when not updating
	assert.True(t, s.canRun())
	// cannot run when running
	assert.False(t, s.canRun())
	// check inner state
	assert.True(t, s.running)
	assert.False(t, s.updating)

	// cannot update when running
	assert.False(t, s.canUpdate())
	// endRun
	s.endRun()
	// check inner state
	assert.False(t, s.running)
	assert.False(t, s.updating)

	// can update when not running
	assert.True(t, s.canUpdate())
	// check inner state
	assert.False(t, s.running)
	assert.True(t, s.updating)
	// cannot run when updating
	assert.False(t, s.canRun())
	// endUpdate
	s.endUpdate()

	// can run after update
	assert.True(t, s.canRun())
}
