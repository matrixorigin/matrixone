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

package protocol

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

const GroupIdentityContract = "expiring-v1"
const GroupIdentityLifetime = 10 * time.Minute
const GroupIdentityRenewalMargin = 5 * time.Minute
const GroupIdentityMaxFuture = GroupIdentityLifetime + GroupIdentityRenewalMargin

// Group identity includes its immutable admission deadline. After this deadline
// every owner rejects new Opens, so an inactive closed fence can be forgotten.
// This is a deduplication contract, not an authentication credential.
func NewGroupIdentity(now time.Time) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return "py1/" + strconv.FormatInt(now.Add(GroupIdentityLifetime).UnixMilli(), 10) + "/" + id.String(), nil
}

func GroupIdentityDeadline(id string) (time.Time, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 3 || parts[0] != "py1" {
		return time.Time{}, errutil.WrapfCauseFirst(ErrProtocol, "invalid expiring group identity")
	}
	millis, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || millis <= 0 || millis > 253402300799999 || strconv.FormatInt(millis, 10) != parts[1] {
		return time.Time{}, errutil.WrapfCauseFirst(ErrProtocol, "invalid group admission deadline")
	}
	parsed, err := uuid.Parse(parts[2])
	if err != nil || parsed.String() != parts[2] {
		return time.Time{}, errutil.WrapfCauseFirst(ErrProtocol, "invalid group identity nonce")
	}
	return time.UnixMilli(millis), nil
}

type groupClock struct {
	mu          sync.Mutex
	start       time.Time
	last        time.Time
	lastElapsed time.Duration
}

var admissionClock = groupClock{start: time.Now()}

// GroupNow never goes backwards and advances with monotonic elapsed time even
// during a wall-clock rollback, including after a forward wall-clock jump.
// Retirement and new admission use this same
// authority, preventing resurrection of a fence already collected by an owner.
func GroupNow() time.Time {
	now := time.Now()
	return admissionClock.read(now, now.Sub(admissionClock.start))
}
func (c *groupClock) read(wall time.Time, elapsed time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.last
	if now.IsZero() {
		now = c.start.Round(0)
	}
	if elapsed > c.lastElapsed {
		now = now.Add(elapsed - c.lastElapsed)
		c.lastElapsed = elapsed
	}
	wall = wall.Round(0)
	if wall.After(now) {
		now = wall
	}
	if c.last.After(now) {
		now = c.last
	}
	c.last = now
	return now
}
