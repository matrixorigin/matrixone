// Copyright 2025 Matrix Origin
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

package merge

import (
	"time"

	"github.com/jonboulle/clockwork"
)

type Clock interface {
	clockwork.Clock
	Until(time.Time) time.Duration
}

type stdClock struct {
	clockwork.Clock
}

func NewStdClock() *stdClock {
	return &stdClock{
		Clock: clockwork.NewRealClock(),
	}
}

func (c stdClock) Until(t time.Time) time.Duration {
	return time.Until(t)
}

type fakeClock struct {
	clockwork.FakeClock
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		FakeClock: clockwork.NewFakeClock(),
	}
}

func (c *fakeClock) Until(t time.Time) time.Duration {
	return t.Sub(c.FakeClock.Now())
}
