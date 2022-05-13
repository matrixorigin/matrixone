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
	"errors"
	"sync/atomic"
)

var (
	ClosedErr = errors.New("closed")
)

type Closable interface {
	IsClosed() bool
	TryClose() bool
}

type ClosedState struct {
	closed int32
}

func (c *ClosedState) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == int32(1)
}

func (c *ClosedState) TryClose() bool {
	if !atomic.CompareAndSwapInt32(&c.closed, int32(0), int32(1)) {
		return false
	}
	return true
}
