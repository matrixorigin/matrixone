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
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	idle  = step(0)
	inUse = step(1)
)

type step int

type checkerStatus struct {
	step  step
	epoch uint64
}

type checkerActivation struct {
	epoch atomic.Uint64
	mu    struct {
		sync.Mutex
		activeScopes int
		permanent    bool
		nextEpoch    uint64
	}
}

func (a *checkerActivation) current() uint64 {
	return a.epoch.Load()
}

func (a *checkerActivation) enablePermanently() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu.permanent {
		return
	}
	a.mu.permanent = true
	if a.epoch.Load() == 0 {
		a.epoch.Store(a.nextEpochLocked())
	}
}

func (a *checkerActivation) beginScope() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu.activeScopes == 0 && !a.mu.permanent {
		a.epoch.Store(a.nextEpochLocked())
	}
	a.mu.activeScopes++
}

func (a *checkerActivation) endScope() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu.activeScopes <= 0 {
		panic("reuse checker scope underflow")
	}
	a.mu.activeScopes--
	if a.mu.activeScopes == 0 && !a.mu.permanent {
		a.epoch.Store(0)
	}
}

func (a *checkerActivation) nextEpochLocked() uint64 {
	a.mu.nextEpoch++
	if a.mu.nextEpoch == 0 {
		a.mu.nextEpoch++
	}
	return a.mu.nextEpoch
}

var checkerActive checkerActivation

type checker[T any, P ReusableObject[T]] struct {
	enable bool
	mu     struct {
		sync.RWMutex
		// we use uintptr as key, to check leak free in gc triggered.
		// We cannot hold the *T in checker.
		m             map[uintptr]checkerStatus
		createStack   map[uintptr]string
		lastFreeStack map[uintptr]string
	}
}

func newChecker[T any, P ReusableObject[T]](enable bool) *checker[T, P] {
	c := &checker[T, P]{
		enable: enable,
	}
	c.mu.m = make(map[uintptr]checkerStatus)
	c.mu.createStack = make(map[uintptr]string)
	c.mu.lastFreeStack = make(map[uintptr]string)
	return c
}

func (c *checker[T, P]) created(v P) {
	epoch := checkerActive.current()
	if epoch == 0 || !c.enable {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	k := uintptr(unsafe.Pointer(v))
	c.mu.m[k] = checkerStatus{step: idle, epoch: epoch}
}

func (c *checker[T, P]) got(v P) {
	epoch := checkerActive.current()
	if epoch == 0 || !c.enable {
		return
	}
	c.gotActive(v, epoch, false)
}

// gotFromPool adopts objects that entered sync.Pool while checker validation
// was disabled. It returns true when the caller must install a finalizer.
func (c *checker[T, P]) gotFromPool(v P) bool {
	epoch := checkerActive.current()
	if epoch == 0 || !c.enable {
		return false
	}
	return c.gotActive(v, epoch, true)
}

func (c *checker[T, P]) gotActive(v P, epoch uint64, adopt bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		if !adopt {
			panic("missing status")
		}
		s = checkerStatus{step: idle, epoch: epoch}
	}

	if s.epoch == epoch && s.step == inUse {
		panic(fmt.Sprintf("double got from pool for type: %T, %+v \n create by: <<<%s>>>\n",
			v, v, c.mu.createStack[k]))
	}
	c.mu.m[k] = checkerStatus{step: inUse, epoch: epoch}
	if enableVerbose.Load() {
		c.mu.createStack[k] = string(debug.Stack())
	}
	return !ok
}

func (c *checker[T, P]) free(v P) {
	epoch := checkerActive.current()
	if epoch == 0 || !c.enable {
		return
	}
	c.freeActive(v, epoch, false)
}

// freeToPool adopts an object acquired before the current checker generation.
// It returns true when the caller must install a finalizer before pooling it.
func (c *checker[T, P]) freeToPool(v P) bool {
	epoch := checkerActive.current()
	if epoch == 0 || !c.enable {
		return false
	}
	return c.freeActive(v, epoch, true)
}

func (c *checker[T, P]) freeActive(v P, epoch uint64, adopt bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		if !adopt {
			return false
		}
		s = checkerStatus{step: inUse, epoch: epoch}
	}

	// An idle object observed in the current generation was already freed.
	// A status from an older generation may have crossed a disabled interval and
	// is therefore adopted instead of being reported as a false double free.
	if s.epoch == epoch && s.step == idle {
		panic(fmt.Sprintf("double free for type: %T, %+v \n create by: <<<%s>>>\n last free by: <<<%s>>> \n",
			v, v, c.mu.createStack[k], c.mu.lastFreeStack[k]))
	}
	c.mu.m[k] = checkerStatus{step: idle, epoch: epoch}
	if enableVerbose.Load() {
		c.mu.lastFreeStack[k] = string(debug.Stack())
	}
	return !ok
}

func (c *checker[T, P]) gc(v P) {
	if !c.enable {
		return
	}

	c.mu.Lock()
	k := uintptr(unsafe.Pointer(v))
	s, ok := c.mu.m[k]
	if !ok {
		c.mu.Unlock()
		return
	}
	createStack := c.mu.createStack[k]
	delete(c.mu.m, k)
	delete(c.mu.createStack, k)
	delete(c.mu.lastFreeStack, k)
	c.mu.Unlock()

	// Finalizers may run after one checker scope ended or while a later scope is
	// active. Only the generation that observed the allocation owns its leak
	// assertion; every generation still owns metadata cleanup.
	if epoch := checkerActive.current(); epoch != 0 &&
		s.epoch == epoch && s.step == inUse {
		panic(fmt.Sprintf("missing free for type: %T, %+v \n create by: <<<%s>>>\n",
			v, v, createStack))
	}
}

func RunReuseTests(fn func()) {
	checkerActive.beginScope()
	defer checkerActive.endScope()
	fn()
	c := make(chan struct{})
	func() {
		v := &waiterGC{
			data: make([]byte, 1024),
		}
		runtime.SetFinalizer(
			v,
			func(v *waiterGC) {
				close(c)
			})
	}()
	debug.FreeOSMemory()
	<-c
}

type waiterGC struct {
	data []byte
}
