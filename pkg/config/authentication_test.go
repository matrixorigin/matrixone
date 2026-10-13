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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateAuthenticationFreshnessBudget(t *testing.T) {
	tests := []struct {
		name           string
		maxClockOffset time.Duration
		connectTimeout time.Duration
		wantError      string
	}{
		{
			name:           "default budget",
			maxClockOffset: 500 * time.Millisecond,
			connectTimeout: time.Minute,
		},
		{
			name:           "pairwise skew exceeds accepted legacy comparison",
			maxClockOffset: time.Second,
			connectTimeout: 1500 * time.Millisecond,
			wantError:      "must be greater than authentication freshness clock budget 2.000000001s",
		},
		{
			name:           "exact clock boundary",
			maxClockOffset: time.Second,
			connectTimeout: 2*time.Second + time.Nanosecond,
			wantError:      "must be greater than authentication freshness clock budget 2.000000001s",
		},
		{
			name:           "strictly above necessary clock boundary",
			maxClockOffset: time.Second,
			connectTimeout: 2*time.Second + 2*time.Nanosecond,
		},
		{
			name:           "negative offset",
			maxClockOffset: -time.Nanosecond,
			connectTimeout: time.Minute,
			wantError:      "max-clock-offset must be positive",
		},
		{
			name:           "budget overflow",
			maxClockOffset: time.Duration(math.MaxInt64),
			connectTimeout: time.Duration(math.MaxInt64),
			wantError:      "too large for authentication freshness budget",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAuthenticationFreshnessBudget(
				tt.maxClockOffset,
				tt.connectTimeout,
			)
			if tt.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}
