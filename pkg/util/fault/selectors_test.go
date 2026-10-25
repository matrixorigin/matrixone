// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fault

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func enableSelectorFaults(t *testing.T) {
	t.Helper()
	started := Enable()
	t.Cleanup(func() {
		for _, name := range []string{"selected", "tenant", "transaction"} {
			if _, err := RemoveFaultPoint(context.Background(), name); err != nil {
				t.Error(err)
			}
		}
		if started {
			Disable()
		}
	})
}

func TestTriggerFaultWithSelectors(t *testing.T) {
	require.False(t, Status())
	_, _, injected := TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
	require.False(t, injected, "disabled faults must remain a no-op")

	for _, tc := range []struct {
		name        string
		tenant      bool
		transaction bool
	}{
		{name: "legacy"},
		{name: "tenant", tenant: true},
		{name: "transaction", transaction: true},
		{name: "both", tenant: true, transaction: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableSelectorFaults(t)
			if tc.tenant {
				require.NoError(t, AddFaultPoint(t.Context(), "tenant", ":::", "ECHO", 7, "", false))
			}
			if tc.transaction {
				require.NoError(t, AddFaultPoint(t.Context(), "transaction", ":::", "ECHO", 0, "accepted", false))
			}
			require.NoError(t, AddFaultPoint(t.Context(), "selected", "1:1::", "ECHO", 23, "payload", false))
			if tc.tenant {
				_, _, injected := TriggerFaultWithSelectors("selected", "tenant", 0, "transaction", "accepted")
				require.False(t, injected)
			}
			if tc.transaction {
				_, _, injected := TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "other")
				require.False(t, injected)
			}
			i, s, injected := TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
			require.True(t, injected, "nonmatching calls must not consume the single trigger")
			require.Equal(t, int64(23), i)
			require.Equal(t, "payload", s)
			_, _, injected = TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
			require.False(t, injected)
		})
	}
}

func TestTriggerFaultSelectorsInstalledWhileCallInFlight(t *testing.T) {
	enableSelectorFaults(t)
	owner := enabled[DomainDefault].Load()
	gate := &faultMap{chIn: make(chan *faultEntry), chOut: make(chan *faultEntry)}
	entered, resume := make(chan struct{}), make(chan struct{})
	forwarded, done := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	var injected bool
	t.Cleanup(func() {
		enabled[DomainDefault].Store(owner)
		release()
		select {
		case <-done:
			close(gate.chIn)
			select {
			case <-forwarded:
			case <-time.After(time.Second):
				t.Error("command forwarder did not exit")
			}
		case <-time.After(time.Second):
			t.Error("in-flight trigger did not exit")
		}
	})
	// Forward real commands, but hold the target command before the map owner
	// evaluates it. Any split selector lookups have already observed old state.
	go func() {
		defer close(forwarded)
		for request := range gate.chIn {
			if request.cmd == TRIGGER && request.name == "selected" {
				close(entered)
				<-resume
			}
			owner.chIn <- request
			gate.chOut <- <-owner.chOut
		}
	}()
	enabled[DomainDefault].Store(gate)
	go func() {
		_, _, injected = TriggerFaultWithSelectors("selected", "tenant", 0, "transaction", "other")
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("target trigger did not reach its owner boundary")
	}
	// Author the new configuration through the real owner while the old call
	// is in flight. The caller must be matched against this same target state.
	enabled[DomainDefault].Store(owner)
	require.NoError(t, AddFaultPoint(t.Context(), "tenant", ":::", "ECHO", 7, "", false))
	require.NoError(t, AddFaultPoint(t.Context(), "transaction", ":::", "ECHO", 0, "accepted", false))
	require.NoError(t, AddFaultPoint(t.Context(), "selected", "1:1::", "ECHO", 0, "barrier", false))
	release()
	select {
	case <-done:
		require.False(t, injected, "old admission must not consume a newly scoped trigger")
	case <-time.After(time.Second):
		t.Fatal("in-flight trigger did not exit")
	}
	_, message, accepted := TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
	require.True(t, accepted)
	require.Equal(t, "barrier", message)
}

func TestTriggerFaultWithSelectorsRejectsNonEcho(t *testing.T) {
	for _, name := range []string{"tenant", "transaction"} {
		t.Run(name, func(t *testing.T) {
			enableSelectorFaults(t)
			require.NoError(t, AddFaultPoint(t.Context(), name, ":::", "RETURN", 7, "accepted", false))
			require.NoError(t, AddFaultPoint(t.Context(), "selected", "1:1::", "ECHO", 0, "", false))
			_, _, injected := TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
			require.False(t, injected)
			require.NoError(t, AddFaultPoint(t.Context(), name, ":::", "ECHO", 7, "accepted", false))
			_, _, injected = TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
			require.True(t, injected)
		})
	}
}

func TestTriggerFaultWithSelectorsWaitRemoval(t *testing.T) {
	enableSelectorFaults(t)
	require.NoError(t, AddFaultPoint(t.Context(), "tenant", ":::", "ECHO", 7, "", false))
	require.NoError(t, AddFaultPoint(t.Context(), "transaction", ":::", "ECHO", 0, "accepted", false))
	require.NoError(t, AddFaultPoint(t.Context(), "selected", "1:1::", "WAIT", 0, "", false))
	_, _, injected := TriggerFaultWithSelectors("selected", "tenant", 0, "transaction", "other")
	require.False(t, injected)

	done := make(chan struct{})
	var accepted bool
	t.Cleanup(func() {
		_, _ = RemoveFaultPoint(context.Background(), "selected")
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("selected WAIT did not exit after removal")
		}
	})
	go func() {
		_, _, accepted = TriggerFaultWithSelectors("selected", "tenant", 7, "transaction", "accepted")
		close(done)
	}()
	entry := lookup(DomainDefault, "selected")
	require.NotNil(t, entry)
	require.Eventually(t, func() bool {
		entry.mutex.Lock()
		defer entry.mutex.Unlock()
		return entry.nWaiters == 1
	}, time.Second, time.Millisecond)
	removed, err := RemoveFaultPoint(t.Context(), "selected")
	require.NoError(t, err)
	require.True(t, removed)
	select {
	case <-done:
		require.True(t, accepted)
	case <-time.After(time.Second):
		t.Fatal("selected WAIT did not exit after removal")
	}
}
