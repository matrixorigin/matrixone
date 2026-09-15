// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/stretchr/testify/require"
)

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
	second.Tuple.GroupID = "second-group"
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
	first.Tuple.GroupID = "reclaim-first"
	first.Tuple.InvocationID = "reclaim-first-invocation"
	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)
	require.NoError(t, firstAdmission.finish(true, protocol.ReasonInputEOF))
	entries, _ := gateway.ledger.Counts()
	require.Equal(t, 1, entries)
	gateway.ledgerTTL = time.Nanosecond

	replay := validInvocation()
	replay.Tuple.GroupID = first.Tuple.GroupID
	replay.Tuple.InvocationID = "reclaim-replay"
	_, err = gateway.admitInvocation(replay)
	require.ErrorIs(t, err, protocol.ErrDuplicate, "a closed group epoch remains fenced after its TTL")

	second := validInvocation()
	second.Tuple.GroupID = "reclaim-second"
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
	first.Tuple.GroupID = "reserved-first"
	first.Tuple.InvocationID = "first"
	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)

	second := validInvocation()
	second.Tuple.GroupID = "reserved-second"
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
	// different group remains rejected until the closed fence is reclaimed by a
	// future owner protocol.
	reentry := validInvocation()
	reentry.Tuple.GroupID = first.Tuple.GroupID
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
	first.Tuple.GroupID = "same-name"
	first.Tuple.InvocationID = "account-one"
	second := validInvocation()
	second.Tuple.AccountID = 2
	second.Tuple.GroupID = first.Tuple.GroupID
	second.Tuple.InvocationID = "account-two"

	firstAdmission, err := gateway.admitInvocation(first)
	require.NoError(t, err)
	secondAdmission, err := gateway.admitInvocation(second)
	require.NoError(t, err)
	require.Len(t, gateway.active, 2)

	require.NoError(t, firstAdmission.finish(true, protocol.ReasonInputEOF))
	require.Len(t, gateway.active, 1)
	gateway.admissionMu.Lock()
	require.Equal(t, uint64(1), gateway.closedGroups[scopedGroupKey(1, "same-name")])
	require.NotContains(t, gateway.closedGroups, scopedGroupKey(2, "same-name"))
	gateway.admissionMu.Unlock()

	require.NoError(t, secondAdmission.finish(true, protocol.ReasonInputEOF))
	gateway.admissionMu.Lock()
	require.Equal(t, uint64(1), gateway.closedGroups[scopedGroupKey(2, "same-name")])
	gateway.admissionMu.Unlock()
}
