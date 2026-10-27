// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiringGroupIdentity(t *testing.T) {
	now := time.Unix(100, 0)
	id, err := NewGroupIdentity(now)
	require.NoError(t, err)
	deadline, err := GroupIdentityDeadline(id)
	require.NoError(t, err)
	require.Equal(t, now.Add(GroupIdentityLifetime).UnixMilli(), deadline.UnixMilli())
	for _, invalid := range []string{
		"legacy", "py1/0/00000000-0000-0000-0000-000000000000",
		"py1/+1/00000000-0000-0000-0000-000000000000",
		"py1/01/00000000-0000-0000-0000-000000000000",
		"py1/9999999999999999999999999999/00000000-0000-0000-0000-000000000000",
		"py1/253402300800000/00000000-0000-0000-0000-000000000000",
		"py1/1000/not-a-uuid", id + "/extra",
	} {
		_, err := GroupIdentityDeadline(invalid)
		require.ErrorIs(t, err, ErrProtocol, invalid)
	}
}

func TestGroupClockDoesNotResurrectExpiredIdentity(t *testing.T) {
	start := time.Unix(100, 0)
	clock := groupClock{start: start}
	require.Equal(t, start.Add(time.Minute), clock.read(start.Add(time.Minute), time.Second))
	// The wall clock rolls back; the previous retirement authority remains.
	require.Equal(t, start.Add(time.Minute+time.Second), clock.read(start.Add(-time.Hour), 2*time.Second))
	// Monotonic progress eventually advances even while wall time stays behind.
	require.Equal(t, start.Add(3*time.Minute-time.Second), clock.read(start.Add(-time.Hour), 2*time.Minute))
}
