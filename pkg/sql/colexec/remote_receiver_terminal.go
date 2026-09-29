// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"context"
	"sync"
)

// RemoteReceiverTerminal belongs to one dispatch registration generation. It
// outlives the pooled operator/process so a late notify can observe its result
// without inspecting a canceled or reused process. Reset publishes the result
// before doing any cleanup that can wait on a remote consumer.
type RemoteReceiverTerminal struct {
	cancel context.CancelCauseFunc
	once   sync.Once
	done   chan struct{}
	err    error
}

func NewRemoteReceiverTerminal(cancel context.CancelCauseFunc) *RemoteReceiverTerminal {
	return &RemoteReceiverTerminal{done: make(chan struct{}), cancel: cancel}
}

func (r *RemoteReceiverTerminal) Finish(err error) {
	r.once.Do(func() {
		r.err = err
		close(r.done)
	})
}

func (r *RemoteReceiverTerminal) Done() <-chan struct{} { return r.done }

// Err may only be read after Done is closed.
func (r *RemoteReceiverTerminal) Err() error { return r.err }

// Cancel controls only this registration generation, even after its Process is reused.
func (r *RemoteReceiverTerminal) Cancel(err error) {
	if r.cancel != nil {
		r.cancel(err)
	}
}
