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

package rscthrottler

import (
	"fmt"
	"os"
	"strings"
)

// MemoryPolicyEnv is an opt-in process environment override for the
// cgroup-aware memory policy. The service config takes precedence when it is
// explicitly set; the environment is useful for controlled benchmark runs.
const MemoryPolicyEnv = "MO_MEMORY_POLICY"

// ReclamationMode separates cgroup/reservation accounting from the active
// reclamation side effects. Both modes retain admission accounting; only the
// accounting+reclamation mode may trigger FreeOSMemory and cache eviction.
type ReclamationMode string

const (
	AccountingOnly           ReclamationMode = "accounting-only"
	AccountingAndReclamation ReclamationMode = "accounting+reclamation"
)

// ResolveReclamationMode resolves an explicit service-config value, or the
// benchmark environment override when the config value is empty. The default
// preserves the production behavior that existed before this diagnostic mode.
func ResolveReclamationMode(configValue string) (ReclamationMode, error) {
	value := strings.TrimSpace(configValue)
	if value == "" {
		value = strings.TrimSpace(os.Getenv(MemoryPolicyEnv))
	}
	if value == "" {
		return AccountingAndReclamation, nil
	}
	switch strings.ToLower(value) {
	case string(AccountingOnly):
		return AccountingOnly, nil
	case string(AccountingAndReclamation), "accounting-and-reclamation":
		return AccountingAndReclamation, nil
	default:
		return "", fmt.Errorf("invalid memory policy %q: expected %q or %q",
			value, AccountingOnly, AccountingAndReclamation)
	}
}

func (m ReclamationMode) EnablesReclamation() bool {
	return m == AccountingAndReclamation
}
