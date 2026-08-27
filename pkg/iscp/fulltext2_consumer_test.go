// Copyright 2026 Matrix Origin
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

package iscp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newFulltext2ConsumerForTest() *IndexConsumer {
	return &IndexConsumer{
		sqlWriter:    newFT2Writer("ngram"),
		sqlBufSendCh: make(chan []byte),
	}
}

func TestRunFulltext2CancellationReportsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errch := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		RunFulltext2(newFulltext2ConsumerForTest(), ctx, errch, &MockRetriever{dtype: ISCPDataType_Snapshot})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunFulltext2 did not stop after context cancellation")
	}
	select {
	case err := <-errch:
		require.ErrorIs(t, err, context.Canceled)
	default:
		t.Fatal("RunFulltext2 did not report context cancellation")
	}
}

func TestRunFulltext2FullErrorChannelDoesNotBlock(t *testing.T) {
	existing := errors.New("existing consumer error")
	errch := make(chan error, 1)
	errch <- existing
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})

	go func() {
		RunFulltext2(newFulltext2ConsumerForTest(), ctx, errch, &MockRetriever{dtype: ISCPDataType_Snapshot})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunFulltext2 blocked while reporting into a full error channel")
	}
	select {
	case err := <-errch:
		require.ErrorIs(t, err, existing)
	default:
		t.Fatal("the existing error was lost from a full error channel")
	}
}
