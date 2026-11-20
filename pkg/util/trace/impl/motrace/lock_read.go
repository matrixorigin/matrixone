// Copyright 2022 Matrix Origin
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

package motrace

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

const (
	lockReadDiagnosticRate    = 1
	lockReadDiagnosticBurst   = 4
	lockReadMinimumSlowTime   = time.Second
	lockReadDiagnosticMessage = "slow locking read"
)

// LockReadLimiter owns a CN's shared, non-blocking diagnostic budget.
type LockReadLimiter struct {
	mu    sync.Mutex
	now   func() time.Time
	last  time.Time
	limit *rate.Limiter
}

func NewLockReadLimiter() *LockReadLimiter {
	return newLockReadLimiter(time.Now, lockReadDiagnosticRate, lockReadDiagnosticBurst)
}

func newLockReadLimiter(now func() time.Time, perSecond rate.Limit, burst int) *LockReadLimiter {
	return &LockReadLimiter{now: now, limit: rate.NewLimiter(perSecond, burst)}
}

func (l *LockReadLimiter) tryAllow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Before(l.last) {
		now = l.last
	}
	l.last = now
	return l.limit.AllowN(now, 1)
}

// SetLockReadLimiter associates the client statement with its CN's budget
// before the statement and request root are published to execution.
func (s *StatementInfo) SetLockReadLimiter(limiter *LockReadLimiter) {
	if s.resourceRoot != nil && limiter != nil {
		s.resourceRoot.EnableLockRead()
		s.lockReadLimiter = limiter
	}
}

func (s *StatementInfo) finishLockRead(summary *resource.StatementResourceSummary, observed resource.LockReadSnapshot, failed bool) {
	if s.lockReadLimiter == nil {
		return
	}
	if !observed.Qualifies(s.Duration, max(lockReadMinimumSlowTime, GetLongQueryTime())) {
		return
	}
	if !s.lockReadLimiter.tryAllow() {
		v2.SlowLockDiagnosticsSuppressedCounter.Inc()
		return
	}
	diagnostic := observed.Diagnostic(uuid.UUID(s.StatementID), summary.AttemptCount, failed)
	summary.LockRead = &diagnostic
	// Keep the statement ID and plan available to correlate with this event.
	s.DisableAgg()
	logutil.GetGlobalLogger().WithOptions(zap.AddStacktrace(zapcore.PanicLevel)).
		Info(lockReadDiagnosticMessage, zap.Any("lock_read", diagnostic))
}
