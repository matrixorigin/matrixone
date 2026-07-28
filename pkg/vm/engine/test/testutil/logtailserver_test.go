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

package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
)

func TestClientSessionLifecycle(t *testing.T) {
	t.Run("session close", func(t *testing.T) {
		session := newTestClientSession(context.Background(), make(chan morpc.Message))
		if err := session.SessionCtx().Err(); err != nil {
			t.Fatalf("new session context is already done: %v", err)
		}
		if err := session.Close(); err != nil {
			t.Fatalf("close session: %v", err)
		}
		if err := session.Close(); err != nil {
			t.Fatalf("close session again: %v", err)
		}
		if err := session.SessionCtx().Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("session context is not canceled after close: %v", err)
		}
	})

	t.Run("transport close", func(t *testing.T) {
		transportCtx, closeTransport := context.WithCancel(context.Background())
		session := newTestClientSession(transportCtx, make(chan morpc.Message))
		defer session.Close()
		closeTransport()
		if err := session.SessionCtx().Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("session context is not canceled with its transport: %v", err)
		}
	})
}
