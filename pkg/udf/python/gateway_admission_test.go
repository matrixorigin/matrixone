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

package python

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/stretchr/testify/require"
)

var testGroupExpiry = time.Now().Add(protocol.GroupIdentityLifetime).UnixMilli()

func testGroupID(label string) string {
	if strings.HasPrefix(label, "py1/") {
		return label
	}
	return fmt.Sprintf("py1/%d/%s", testGroupExpiry, uuid.NewSHA1(uuid.Nil, []byte(label)))
}

func newAdmissionTestGateway(t *testing.T, maxActive int) *Gateway {
	t.Helper()
	gateway, err := NewGateway(ClientConfig{
		Enabled:              true,
		AllowUnisolated:      true,
		ServerAddress:        "127.0.0.1:1",
		MaxActiveInvocations: maxActive,
		MaxTerminalEntries:   8,
		MaxTerminalBytes:     1 << 20,
		TerminalRecordTTL:    time.Minute,
	})
	require.NoError(t, err)
	return gateway
}

func TestGatewayAdmissionFencesDuplicateAndReleasesOnce(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	in := validInvocation()
	admission, err := gateway.admitInvocation(in)
	require.NoError(t, err)
	registered, active, inFlight := admission.group.Counts()
	require.Equal(t, 1, registered)
	require.Equal(t, 1, active)
	require.Equal(t, 0, inFlight)

	_, err = gateway.admitInvocation(in)
	require.ErrorIs(t, err, protocol.ErrDuplicate)
	entries, _ := gateway.ledger.Counts()
	require.Equal(t, 1, entries)
	require.Len(t, gateway.active, 1)

	require.NoError(t, admission.finish(true, protocol.ReasonInputEOF))
	require.NoError(t, admission.finish(true, protocol.ReasonInputEOF))
	entries, _ = gateway.ledger.Counts()
	require.Equal(t, 1, entries, "accepted execution keeps its tombstone")
	require.Len(t, gateway.active, 0, "group owner releases K exactly once")
	require.Equal(t, protocol.GroupReleased, admission.group.State())
	_, err = gateway.admitInvocation(in)
	require.ErrorIs(t, err, protocol.ErrDuplicate)
}

func TestGatewayAdmissionRejectsAfterClose(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	require.NoError(t, gateway.Close())

	_, err := gateway.admitInvocation(validInvocation())
	require.ErrorIs(t, err, errGatewayClosed)
	if gateway.ledger != nil {
		entries, _ := gateway.ledger.Counts()
		require.Zero(t, entries)
	}
	require.Len(t, gateway.active, 0)
}

func TestGatewayAdmissionRejectsAtKWithoutLeavingLedgerEntry(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	first := validInvocation()
	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)

	second := validInvocation()
	second.Tuple.GroupID = testGroupID("second-group")
	second.Tuple.InvocationID = "second-invocation"
	_, err = gateway.admitInvocation(second)
	require.ErrorContains(t, err, "active invocation slots are full")
	entries, _ := gateway.ledger.Counts()
	require.Equal(t, 1, entries, "K rejection abandons the speculative ledger entry")
	require.Len(t, gateway.active, 1)

	require.NoError(t, firstAdmission.finish(true, protocol.ReasonFailure))
	secondAdmission, err := gateway.admitInvocation(second)
	require.NoError(t, err)
	require.NoError(t, secondAdmission.finish(false, protocol.ReasonPartialOpenError))
	entries, _ = gateway.ledger.Counts()
	require.Equal(t, 1, entries, "a pre-send failure abandons instead of tombstoning")
	require.Len(t, gateway.active, 0)
}

func TestGatewayAdmissionReclaimsOnlyClosedEpochTombstones(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	gateway.cfg.MaxTerminalEntries = 1
	gateway.ledgerTTL = time.Nanosecond
	first := validInvocation()
	first.Tuple.GroupID = testGroupID("reclaim-first")
	first.Tuple.InvocationID = "reclaim-first-invocation"
	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)
	require.NoError(t, firstAdmission.finish(true, protocol.ReasonInputEOF))
	entries, _ := gateway.ledger.Counts()
	require.Equal(t, 1, entries)
	gateway.ledgerTTL = time.Nanosecond

	replay := validInvocation()
	replay.Tuple.GroupID = testGroupID(first.Tuple.GroupID)
	replay.Tuple.InvocationID = "reclaim-replay"
	_, err = gateway.admitInvocation(replay)
	require.ErrorIs(t, err, protocol.ErrDuplicate, "a closed group epoch remains fenced after its TTL")

	second := validInvocation()
	second.Tuple.GroupID = testGroupID("reclaim-second")
	second.Tuple.InvocationID = "reclaim-second-invocation"
	secondAdmission, err := gateway.admitInvocation(second)
	require.NoError(t, err, "an owner-closed tombstone can release bounded ledger capacity")
	require.NoError(t, secondAdmission.finish(false, protocol.ReasonPartialOpenError))
	entries, _ = gateway.ledger.Counts()
	require.Zero(t, entries)
}

func TestGatewayAdmissionAcceptedSendIsTerminalOnTransportFailure(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	in := validInvocation()
	admission, err := gateway.admitInvocation(in)
	require.NoError(t, err)
	require.NoError(t, admission.finish(true, protocol.ReasonFailure))
	_, err = gateway.admitInvocation(in)
	require.Error(t, err)
	require.True(t, errors.Is(err, protocol.ErrDuplicate))
	require.Len(t, gateway.active, 0)
}

func TestGatewayAdmissionRetriesOwnerCleanupAfterGroupReleaseFailure(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	require.NoError(t, gateway.ensureAdmissionState())

	var releaseAttempts int
	group, err := protocol.NewExecutionGroup(
		"release-retry", 1, 1,
		func() error {
			releaseAttempts++
			if releaseAttempts <= 2 {
				return errors.New("injected group release failure")
			}
			return nil
		},
	)
	require.NoError(t, err)
	token, err := group.BeginOpen("member")
	require.NoError(t, err)
	require.NoError(t, token.Commit())
	credit, err := gateway.ledger.Reserve("release-retry", 1, 1, 16)
	require.NoError(t, err)
	require.NoError(t, credit.Add("release-retry-key", 16, time.Now().Add(time.Minute)))

	admission := &invocationAdmission{
		gateway: gateway, group: group, credit: credit,
		key: "release-retry-key", groupKey: "release-retry", invocationID: "member", memberOpen: true,
	}
	require.ErrorContains(t, admission.finish(true, protocol.ReasonFailure), "injected group release failure")
	require.Equal(t, protocol.GroupDraining, group.State())
	require.NoError(t, admission.finish(true, protocol.ReasonFailure))
	require.Equal(t, 3, releaseAttempts)
	require.Equal(t, protocol.GroupReleased, group.State())
	require.NoError(t, admission.finish(true, protocol.ReasonFailure))
}

func TestGatewayAdmissionReservesClosedGroupCapacityWhileActive(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 2)
	gateway.closedGroupLimit = 1
	gateway.closedGroupByteLimit = 1 << 20

	first := validInvocation()
	first.Tuple.GroupID = testGroupID("reserved-first")
	first.Tuple.InvocationID = "first"
	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)

	second := validInvocation()
	second.Tuple.GroupID = testGroupID("reserved-second")
	second.Tuple.InvocationID = "second"
	_, err = gateway.admitInvocation(second)
	require.ErrorIs(t, err, protocol.ErrLedgerFull)
	gateway.admissionMu.Lock()
	require.Len(t, gateway.admittedGroups, 1)
	require.Positive(t, gateway.reservedGroupBytes)
	gateway.admissionMu.Unlock()

	require.NoError(t, firstAdmission.finish(true, protocol.ReasonInputEOF))
	gateway.admissionMu.Lock()
	require.Empty(t, gateway.admittedGroups)
	require.Zero(t, gateway.reservedGroupBytes)
	require.Equal(t, uint64(1), gateway.closedGroups[scopedGroupKey(first.Tuple.AccountID, first.Tuple.GroupID)])
	gateway.admissionMu.Unlock()

	// A new epoch of the same group reuses its already-reserved fence. A
	// different group remains rejected until the immutable group deadline passes.
	reentry := validInvocation()
	reentry.Tuple.GroupID = testGroupID(first.Tuple.GroupID)
	reentry.Tuple.GroupEpoch = 2
	reentry.Tuple.InvocationID = "reentry"
	reentryAdmission, err := gateway.admitInvocation(reentry)
	require.NoError(t, err)
	require.NoError(t, reentryAdmission.finish(false, protocol.ReasonPartialOpenError))

	_, err = gateway.admitInvocation(second)
	require.ErrorIs(t, err, protocol.ErrLedgerFull)
}

func TestGatewayAdmissionScopesGroupByAccount(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 2)
	first := validInvocation()
	first.Tuple.GroupID = testGroupID("same-name")
	first.Tuple.InvocationID = "account-one"
	second := validInvocation()
	second.Tuple.AccountID = 2
	second.Tuple.GroupID = testGroupID(first.Tuple.GroupID)
	second.Tuple.InvocationID = "account-two"

	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)
	secondAdmission, err := gateway.admitInvocation(second)
	require.NoError(t, err)
	require.Len(t, gateway.active, 2)

	require.NoError(t, firstAdmission.finish(true, protocol.ReasonInputEOF))
	require.Len(t, gateway.active, 1)
	gateway.admissionMu.Lock()
	require.Equal(t, uint64(1), gateway.closedGroups[scopedGroupKey(1, testGroupID("same-name"))])
	require.NotContains(t, gateway.closedGroups, scopedGroupKey(2, testGroupID("same-name")))
	gateway.admissionMu.Unlock()

	require.NoError(t, secondAdmission.finish(true, protocol.ReasonInputEOF))
	gateway.admissionMu.Lock()
	require.Equal(t, uint64(1), gateway.closedGroups[scopedGroupKey(2, testGroupID("same-name"))])
	gateway.admissionMu.Unlock()
}

func TestGatewayAdmissionRetiresExpiredGroups(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 2)
	gateway.closedGroupLimit = 2
	gateway.ledgerTTL = time.Nanosecond
	now := protocol.GroupNow()
	gateway.groupNow = func() time.Time { return now }
	var oldID, oldestID string
	for window := 0; window < 3; window++ {
		for i := 0; i < 2; i++ {
			in := validInvocation()
			var err error
			in.Tuple.GroupID, err = protocol.NewGroupIdentity(now)
			require.NoError(t, err)
			oldID = in.Tuple.GroupID
			if oldestID == "" {
				oldestID = oldID
			}
			admission, err := gateway.admitInvocation(in)
			require.NoError(t, err)
			require.NoError(t, admission.finish(true, protocol.ReasonInputEOF))
			require.LessOrEqual(t, len(gateway.closedGroups), 2)
		}
		now = now.Add(protocol.GroupIdentityLifetime + time.Second)
		for _, epoch := range []uint64{1, 2} {
			replay := validInvocation()
			replay.Tuple.GroupID = oldID
			replay.Tuple.GroupEpoch = epoch
			replay.Tuple.InvocationID = "late-new-member"
			_, err := gateway.admitInvocation(replay)
			require.ErrorIs(t, err, protocol.ErrGroupClosed)
		}
	}
	fresh := validInvocation()
	var err error
	fresh.Tuple.GroupID, err = protocol.NewGroupIdentity(now)
	require.NoError(t, err)
	admitted, err := gateway.admitInvocation(fresh)
	require.NoError(t, err)
	require.NoError(t, admitted.finish(false, protocol.ReasonPartialOpenError))
	require.NotContains(t, gateway.closedGroups, scopedGroupKey(1, oldestID))
	replay := validInvocation()
	replay.Tuple.GroupID, replay.Tuple.GroupEpoch = oldestID, 2
	_, err = gateway.admitInvocation(replay)
	require.ErrorIs(t, err, protocol.ErrGroupClosed)
	require.Empty(t, gateway.active)
	require.Empty(t, gateway.admittedGroups)
	require.Zero(t, gateway.reservedGroupBytes)
	// Collection happened in later windows; the original IDs cannot reopen
	// even after both their terminal records and closed fences are absent.
	require.LessOrEqual(t, gateway.closedGroupBytes, int64(2*(len(scopedGroupKey(1, oldID))+8)))
}

func TestGatewayAdmissionKeepsActiveExpiredGroup(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 2)
	gateway.closedGroupLimit = 2
	gateway.ledgerTTL = time.Nanosecond
	now := protocol.GroupNow()
	gateway.groupNow = func() time.Time { return now }
	first := validInvocation()
	var err error
	first.Tuple.GroupID, err = protocol.NewGroupIdentity(now)
	require.NoError(t, err)
	a, err := gateway.admitInvocation(first)
	require.NoError(t, err)
	require.NoError(t, a.finish(true, protocol.ReasonInputEOF))
	first.Tuple.GroupEpoch++
	first.Tuple.InvocationID = "active-next-epoch"
	a, err = gateway.admitInvocation(first)
	require.NoError(t, err)
	now = now.Add(protocol.GroupIdentityLifetime + time.Second)
	second := validInvocation()
	second.Tuple.GroupID, err = protocol.NewGroupIdentity(now)
	require.NoError(t, err)
	b, err := gateway.admitInvocation(second)
	require.NoError(t, err)
	require.Len(t, gateway.active, 2, "expiry must not release live K")
	require.Contains(t, gateway.closedGroups, scopedGroupKey(1, first.Tuple.GroupID))
	require.NoError(t, a.finish(true, protocol.ReasonInputEOF))
	require.NoError(t, a.finish(true, protocol.ReasonInputEOF))
	require.NoError(t, b.finish(false, protocol.ReasonPartialOpenError))
	require.Empty(t, gateway.active)
	third := validInvocation()
	third.Tuple.GroupID, err = protocol.NewGroupIdentity(now)
	require.NoError(t, err)
	c, err := gateway.admitInvocation(third)
	require.NoError(t, err)
	require.NotContains(t, gateway.closedGroups, scopedGroupKey(1, first.Tuple.GroupID))
	require.NoError(t, c.finish(false, protocol.ReasonPartialOpenError))
	require.Zero(t, gateway.reservedGroupBytes)
}

func TestGatewayAdmissionRejectsInvalidGroupDeadline(t *testing.T) {
	gateway := newAdmissionTestGateway(t, 1)
	now := time.Unix(100, 0)
	gateway.groupNow = func() time.Time { return now }
	expired, err := protocol.NewGroupIdentity(now.Add(-protocol.GroupIdentityLifetime))
	require.NoError(t, err)
	farFuture, err := protocol.NewGroupIdentity(now.Add(protocol.GroupIdentityRenewalMargin + time.Millisecond))
	require.NoError(t, err)
	for _, id := range []string{"legacy-group", expired, farFuture} {
		in := validInvocation()
		in.Tuple.GroupID = id
		_, err := gateway.admitInvocation(in)
		require.Error(t, err)
		require.Empty(t, gateway.admittedGroups)
		require.Empty(t, gateway.active)
		require.Zero(t, gateway.reservedGroupBytes)
	}
	boundary, err := protocol.NewGroupIdentity(now.Add(protocol.GroupIdentityRenewalMargin))
	require.NoError(t, err)
	in := validInvocation()
	in.Tuple.GroupID = boundary
	a, err := gateway.admitInvocation(in)
	require.NoError(t, err, "exact maximum future deadline remains admissible")
	require.NoError(t, a.finish(false, protocol.ReasonPartialOpenError))
}
