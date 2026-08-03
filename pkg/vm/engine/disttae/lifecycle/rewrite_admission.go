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

package lifecycle

import (
	"fmt"
	"math"
	"sync"
	"time"

	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type RewriteReleaseProfile struct {
	Window                   time.Duration
	MaxAmplification         float64
	MaxSourceBytesPerAccount uint64
	MaxSourceBytesPerCluster uint64
}

type RewriteAdmission struct {
	mu sync.Mutex

	profile      RewriteReleaseProfile
	windowStart  time.Time
	clusterBytes uint64
	accountBytes map[uint32]uint64
}

func NewRewriteAdmission(profile RewriteReleaseProfile) (*RewriteAdmission, error) {
	if profile.Window <= 0 ||
		profile.MaxAmplification <= 0 ||
		profile.MaxSourceBytesPerAccount == 0 ||
		profile.MaxSourceBytesPerCluster == 0 {
		return nil, fmt.Errorf("Lifecycle Rewrite release profile is incomplete")
	}
	return &RewriteAdmission{
		profile:      profile,
		accountBytes: make(map[uint32]uint64),
	}, nil
}

func (admission *RewriteAdmission) CheckAmplification(
	sourcePressureBytes uint64,
	retiredPressureBytes uint64,
) error {
	if sourcePressureBytes == 0 || retiredPressureBytes == 0 {
		return fmt.Errorf("MIXED_LAYOUT_BLOCKED: expired Rewrite bytes are zero")
	}
	amplification := float64(sourcePressureBytes) /
		float64(retiredPressureBytes)
	if math.IsInf(amplification, 0) ||
		math.IsNaN(amplification) ||
		amplification > admission.profile.MaxAmplification {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"rewrite_amplification",
		).Inc()
		return fmt.Errorf(
			"MIXED_LAYOUT_BLOCKED: rewrite amplification %.2f exceeds %.2f",
			amplification,
			admission.profile.MaxAmplification,
		)
	}
	return nil
}

// ReserveSource is called before the source Reader or Merge writer starts.
// Charges are intentionally not refunded: MO already paid the discovery/read
// pressure and retries must not bypass a fixed-window cap.
func (admission *RewriteAdmission) ReserveSource(
	accountID uint32,
	sourceBytes uint64,
	now time.Time,
) error {
	if accountID == 0 || sourceBytes == 0 || now.IsZero() {
		return fmt.Errorf("RESOURCE_BLOCKED: Rewrite reservation is incomplete")
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	window := now.Truncate(admission.profile.Window)
	if admission.windowStart.IsZero() || !window.Equal(admission.windowStart) {
		admission.windowStart = window
		admission.clusterBytes = 0
		clear(admission.accountBytes)
	}
	accountBytes, overflow := addUint64(
		admission.accountBytes[accountID],
		sourceBytes,
	)
	if overflow || accountBytes > admission.profile.MaxSourceBytesPerAccount {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"rewrite_account_bytes",
		).Inc()
		return fmt.Errorf("RESOURCE_BLOCKED: account Rewrite byte window exhausted")
	}
	clusterBytes, overflow := addUint64(
		admission.clusterBytes,
		sourceBytes,
	)
	if overflow || clusterBytes > admission.profile.MaxSourceBytesPerCluster {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"rewrite_cluster_bytes",
		).Inc()
		return fmt.Errorf("RESOURCE_BLOCKED: cluster Rewrite byte window exhausted")
	}
	admission.accountBytes[accountID] = accountBytes
	admission.clusterBytes = clusterBytes
	return nil
}

func addUint64(left, right uint64) (uint64, bool) {
	value := left + right
	return value, value < left
}
