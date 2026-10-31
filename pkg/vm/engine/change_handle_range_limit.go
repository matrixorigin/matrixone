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

package engine

import (
	"context"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ChangeRangeLimit is an opt-in memory bound for range replay. Callers that
// also provide ChangeRangeSpillConfig spill excess replay rows; a zero value
// means no limit, preserving existing CollectChanges callers. A row bound
// cannot bound variable-width persisted columns by itself, so range-aware
// persisted replay rejects MaxInMemoryRows without MaxInMemoryBytes.
type ChangeRangeLimit struct {
	MaxInMemoryRows  int
	MaxInMemoryBytes int
}

// Enabled reports whether the caller opted into either bound.
func (l ChangeRangeLimit) Enabled() bool {
	return l.MaxInMemoryRows > 0 || l.MaxInMemoryBytes > 0
}

type changeRangeLimitContextKey struct{}

// WithChangeRangeLimit attaches an explicit caller-owned recovery bound.
func WithChangeRangeLimit(ctx context.Context, limit ChangeRangeLimit) context.Context {
	if ctx == nil || !limit.Enabled() {
		return ctx
	}
	return context.WithValue(ctx, changeRangeLimitContextKey{}, limit)
}

// ChangeRangeLimitFromContext returns the caller-owned recovery bound, or its
// zero value when the caller did not opt in.
func ChangeRangeLimitFromContext(ctx context.Context) ChangeRangeLimit {
	if ctx == nil {
		return ChangeRangeLimit{}
	}
	limit, _ := ctx.Value(changeRangeLimitContextKey{}).(ChangeRangeLimit)
	return limit
}

// ChangeRangeSpillReservation owns one query-scoped spill resource charge.
type ChangeRangeSpillReservation interface {
	Release() bool
}

// ChangeRangeGrowingSpillReservation owns a spill charge that grows with a
// single open file.
type ChangeRangeGrowingSpillReservation interface {
	ChangeRangeSpillReservation
	Grow(uint64) error
}

// ChangeRangeSpillConfig supplies anonymous query-scoped files and admission
// controls when an opted-in range exceeds its memory limit.
type ChangeRangeSpillConfig struct {
	FileFactory  func(context.Context, string) (*os.File, error)
	ReserveDisk  func(uint64) (ChangeRangeGrowingSpillReservation, error)
	ReserveFiles func(uint64) (ChangeRangeSpillReservation, error)
}

// Enabled reports whether all spill ownership hooks are available.
func (c ChangeRangeSpillConfig) Enabled() bool {
	return c.FileFactory != nil && c.ReserveDisk != nil && c.ReserveFiles != nil
}

type changeRangeSpillContextKey struct{}

// WithChangeRangeSpill attaches spill ownership for an explicitly bounded
// change range.
func WithChangeRangeSpill(ctx context.Context, config ChangeRangeSpillConfig) context.Context {
	if ctx == nil || !config.Enabled() {
		return ctx
	}
	return context.WithValue(ctx, changeRangeSpillContextKey{}, config)
}

// ChangeRangeSpillFromContext returns the caller-owned spill configuration.
func ChangeRangeSpillFromContext(ctx context.Context) ChangeRangeSpillConfig {
	if ctx == nil {
		return ChangeRangeSpillConfig{}
	}
	config, _ := ctx.Value(changeRangeSpillContextKey{}).(ChangeRangeSpillConfig)
	return config
}

// changeRangeCapacityMessage marks the capacity class: an attached
// ChangeRangeLimit was exhausted while assembling a replay stream. The class
// is distinct from caller misconfiguration (validation errors) and from
// integrity errors (stale or unservable history): consumers that attached the
// bound recognize it to take their own capacity lane instead of treating the
// read as corrupt.
const changeRangeCapacityMessage = "change range exceeded its attached in-memory bound"

// NewChangeRangeCapacityError reports exhaustion of an attached
// ChangeRangeLimit during replay assembly. It is a capacity-class error:
// nothing about the underlying stream was unreadable or corrupt, and retrying
// with a larger (or no) attached bound serves the identical rows.
func NewChangeRangeCapacityError(detail string) error {
	return moerr.NewInternalErrorNoCtxf("%s: %s", changeRangeCapacityMessage, detail)
}

// IsChangeRangeCapacityError reports whether err belongs to the capacity
// class produced by NewChangeRangeCapacityError.
func IsChangeRangeCapacityError(err error) bool {
	return err != nil && strings.Contains(err.Error(), changeRangeCapacityMessage)
}
