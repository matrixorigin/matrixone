// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	internalTimeout = time.Second * 10
	oneWayTimeout   = time.Second * 600
)

var (
	InternalWrite = WriteOptions{}.Internal()
	SyncWrite     = WriteOptions{}
	AsyncWrite    = WriteOptions{}.Async()
)

type WriteOptions struct {
	async          bool
	chunk          bool
	lastChunk      bool
	internal       bool
	stream         bool
	streamSequence uint32
}

func (opts WriteOptions) Async() WriteOptions {
	opts.async = true
	return opts
}

func (opts WriteOptions) Chunk(last bool) WriteOptions {
	opts.chunk = true
	opts.lastChunk = last
	return opts
}

func (opts WriteOptions) Internal() WriteOptions {
	opts.internal = true
	return opts
}

func (opts WriteOptions) Stream(sequence uint32) WriteOptions {
	opts.stream = true
	opts.streamSequence = sequence
	return opts
}

// Timeout return true if the message is timeout
func (m RPCMessage) Timeout() bool {
	select {
	case <-m.Ctx.Done():
		return true
	default:
		return false
	}
}

// GetTimeoutFromContext returns the timeout duration from context.
func (m RPCMessage) GetTimeoutFromContext() (time.Duration, error) {
	if m.opts.internal {
		return internalTimeout, nil
	}
	if m.opts.async {
		return oneWayTimeout, nil
	}

	d, ok := m.Ctx.Deadline()
	if !ok {
		return 0, moerr.NewInvalidInputNoCtx("timeout deadline not set")
	}
	now := time.Now()
	if now.After(d) {
		return 0, moerr.NewInvalidInputNoCtx("timeout has invalid deadline")
	}
	return d.Sub(now), nil
}
