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

package iceberg

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
)

const (
	defaultOrphanSweepAccountsPerTick = 16
	defaultOrphanSweepPerAccountLimit = 100
)

type OrphanSweepAccountSource interface {
	ListOrphanSweepAccounts(ctx context.Context, limit int) ([]uint32, error)
}

type OrphanSweepOwner interface {
	OwnsOrphanSweep(ctx context.Context, accountID uint32) (bool, error)
}

type OrphanSweepRunner interface {
	SweepOrphans(ctx context.Context, accountID uint32, limit int) (maintenance.SweepResult, error)
}

type OrphanSweepRunnerFunc func(ctx context.Context, accountID uint32, limit int) (maintenance.SweepResult, error)

func (fn OrphanSweepRunnerFunc) SweepOrphans(ctx context.Context, accountID uint32, limit int) (maintenance.SweepResult, error) {
	return fn(ctx, accountID, limit)
}

type OrphanSweepErrorHandler func(ctx context.Context, err error)

type OrphanSweepSchedulerOptions struct {
	Accounts           OrphanSweepAccountSource
	Owner              OrphanSweepOwner
	Runner             OrphanSweepRunner
	Interval           time.Duration
	MaxAccountsPerTick int
	PerAccountLimit    int
	OnError            OrphanSweepErrorHandler
}

type OrphanSweepScheduler struct {
	opts OrphanSweepSchedulerOptions
}

type OrphanSweepSchedulerResult struct {
	AccountsScanned int
	AccountsOwned   int
	AccountsSkipped int
	AccountsFailed  int
	OrphansScanned  int
	OrphansDeleted  int
	OrphansFailed   int
}

func NewOrphanSweepScheduler(opts OrphanSweepSchedulerOptions) OrphanSweepScheduler {
	return OrphanSweepScheduler{opts: opts}
}

func (s OrphanSweepScheduler) RunOnce(ctx context.Context) (OrphanSweepSchedulerResult, error) {
	if err := s.validate(); err != nil {
		return OrphanSweepSchedulerResult{}, err
	}
	accountLimit := s.maxAccountsPerTick()
	accounts, err := s.opts.Accounts.ListOrphanSweepAccounts(ctx, accountLimit)
	if err != nil {
		return OrphanSweepSchedulerResult{}, err
	}
	if len(accounts) > accountLimit {
		accounts = accounts[:accountLimit]
	}
	result := OrphanSweepSchedulerResult{AccountsScanned: len(accounts)}
	var firstErr error
	for _, accountID := range accounts {
		if accountID == 0 {
			result.AccountsSkipped++
			continue
		}
		if s.opts.Owner != nil {
			owned, err := s.opts.Owner.OwnsOrphanSweep(ctx, accountID)
			if err != nil {
				result.AccountsFailed++
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !owned {
				result.AccountsSkipped++
				continue
			}
		}
		result.AccountsOwned++
		sweep, err := s.opts.Runner.SweepOrphans(ctx, accountID, s.perAccountLimit())
		if err != nil {
			result.AccountsFailed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		result.OrphansScanned += sweep.Scanned
		result.OrphansDeleted += sweep.Deleted
		result.OrphansFailed += sweep.Failed
	}
	return result, firstErr
}

func (s OrphanSweepScheduler) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	if s.opts.Interval <= 0 {
		return moerr.NewInvalidInput(ctx, "iceberg orphan sweep scheduler requires a positive interval")
	}
	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		if _, err := s.RunOnce(ctx); err != nil && s.opts.OnError != nil {
			s.opts.OnError(ctx, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s OrphanSweepScheduler) validate() error {
	if s.opts.Accounts == nil {
		return moerr.NewInternalErrorNoCtx("iceberg orphan sweep scheduler requires an account source")
	}
	if s.opts.Runner == nil {
		return moerr.NewInternalErrorNoCtx("iceberg orphan sweep scheduler requires a runner")
	}
	return nil
}

func (s OrphanSweepScheduler) maxAccountsPerTick() int {
	if s.opts.MaxAccountsPerTick > 0 {
		return s.opts.MaxAccountsPerTick
	}
	return defaultOrphanSweepAccountsPerTick
}

func (s OrphanSweepScheduler) perAccountLimit() int {
	if s.opts.PerAccountLimit > 0 {
		return s.opts.PerAccountLimit
	}
	return defaultOrphanSweepPerAccountLimit
}
