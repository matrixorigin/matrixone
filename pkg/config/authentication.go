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

package config

import (
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// Authentication captures local-now+max-offset. When the serving CN is
	// max-offset ahead of the TN, the TN clock can need another max-offset to
	// reach that fence. Keep separate fixed reserves for pre-authentication
	// protocol work and for publishing/applying the first logtail progress after
	// the physical fence is reached.
	authenticationHandshakeHeadroom = time.Second
	authenticationLogtailHeadroom   = time.Second
)

// ValidateAuthenticationFreshnessBudget rejects a CN connection deadline that
// cannot cover the worst healthy pairwise-clock fence plus minimum protocol and
// logtail progress headroom. Authentication transaction creation is owned by
// this connection deadline; CreateTxnOpTimeout intentionally does not shorten
// it.
func ValidateAuthenticationFreshnessBudget(
	maxClockOffset time.Duration,
	connectTimeout time.Duration,
) error {
	if maxClockOffset < 0 {
		return moerr.NewBadConfigNoCtx("max-clock-offset must be positive")
	}

	const fixedBudget = time.Nanosecond +
		authenticationHandshakeHeadroom +
		authenticationLogtailHeadroom
	if maxClockOffset > (time.Duration(math.MaxInt64)-fixedBudget)/2 {
		return moerr.NewBadConfigNoCtx(
			"max-clock-offset is too large for authentication freshness budget")
	}

	minimum := 2*maxClockOffset + fixedBudget
	if connectTimeout <= minimum {
		return moerr.NewBadConfigNoCtxf(
			"cn.frontend.connectTimeout %s must be greater than authentication freshness budget %s (2*max-clock-offset + 1ns fence + 1s handshake + 1s logtail headroom)",
			connectTimeout,
			minimum,
		)
	}
	return nil
}
