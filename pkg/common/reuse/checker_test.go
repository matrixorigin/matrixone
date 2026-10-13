// Copyright 2023 Matrix Origin
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

package reuse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDoubleFree(t *testing.T) {
	RunReuseTests(func() {
		c := newChecker[person](true)
		p := &person{}
		c.created(p)

		c.got(p)
		c.free(p)

		defer func() {
			assert.NotNil(t, recover())
		}()
		c.free(p)
	})
}

func TestLeakFree(t *testing.T) {
	RunReuseTests(func() {
		c := newChecker[person](true)
		p := &person{}
		c.created(p)
		c.got(p)

		defer func() {
			assert.NotNil(t, recover())
		}()
		c.gc(p)
	})
}

func TestCheckerActivationScopes(t *testing.T) {
	var activation checkerActivation
	activation.beginScope()
	epoch := activation.current()
	assert.NotZero(t, epoch)

	activation.beginScope()
	assert.Equal(t, epoch, activation.current())
	activation.endScope()
	assert.Equal(t, epoch, activation.current())

	activation.endScope()
	assert.Zero(t, activation.current())
	activation.beginScope()
	assert.Greater(t, activation.current(), epoch)
	activation.endScope()
}

func TestCheckerActivationOverlappingScopes(t *testing.T) {
	var activation checkerActivation
	activation.beginScope()
	epoch := activation.current()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		activation.beginScope()
		close(started)
		<-release
		activation.endScope()
		close(done)
	}()

	<-started
	activation.endScope()
	assert.Equal(t, epoch, activation.current())
	close(release)
	<-done
	assert.Zero(t, activation.current())
}

func TestCheckerActivationPreservesPermanentMode(t *testing.T) {
	var activation checkerActivation
	activation.enablePermanently()
	epoch := activation.current()
	assert.NotZero(t, epoch)

	activation.beginScope()
	activation.endScope()
	assert.Equal(t, epoch, activation.current())
}

func TestCheckerGCCleansDiagnosticMetadata(t *testing.T) {
	oldVerbose := enableVerbose.Swap(true)
	defer enableVerbose.Store(oldVerbose)

	RunReuseTests(func() {
		c := newChecker[person](true)
		p := &person{}
		c.created(p)
		c.got(p)
		c.free(p)
		c.gc(p)

		assert.Empty(t, c.mu.m)
		assert.Empty(t, c.mu.createStack)
		assert.Empty(t, c.mu.lastFreeStack)
	})
}

func TestCheckerAcceptsStatusFromOlderGeneration(t *testing.T) {
	c := newChecker[person](true)
	p := &person{}
	RunReuseTests(func() {
		c.created(p)
		c.got(p)
		c.mu.Lock()
		for key, status := range c.mu.m {
			status.epoch = 0
			c.mu.m[key] = status
		}
		c.mu.Unlock()
		assert.NotPanics(t, func() {
			c.got(p)
		})
		c.free(p)
	})
}
