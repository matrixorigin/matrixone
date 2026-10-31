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

package schedule

// TemporalAccessIntent captures one statement's placement and offload inputs.
// It is a value snapshot, not authority to bypass the normal scheduler.
type TemporalAccessIntent struct {
	Scheduling SchedulingIntent
	CurrentCN  Worker
	Sirius     bool
}

// AllowsFixedTemporalCandidate proves offload compatibility and a known local
// owner. Fixed-offset transport and the ordinary scheduler are separate checks.
func (intent TemporalAccessIntent) AllowsFixedTemporalCandidate() bool {
	return !intent.Sirius && hasWorkerIdentity(intent.CurrentCN) && intent.CurrentCN.Route == WorkerRouteLocal &&
		ValidateSchedulingIntent(intent.Scheduling) == ""
}

// AllowsNamedTemporalCandidate uses the scheduler's actual local decision.
// A missing current identity is not a sufficient placement proof, even when
// legacy local scheduling happens to allow it.
func (intent TemporalAccessIntent) AllowsNamedTemporalCandidate() bool {
	if !intent.AllowsFixedTemporalCandidate() {
		return false
	}
	// Local placement does not resolve an explicit pool. Until that pool is
	// proved compatible, changing a multi-CN query to local may violate it.
	policy := intent.Scheduling
	if policy.Explicit || policy.RequestedPool != "" || policy.PoolFallback != PoolFallbackLegacyCompatible ||
		policy.EmptyWorkerPolicy != EmptyWorkerLocalFallback || policy.CurrentCNPolicy != CurrentCNAllowed ||
		policy.WorkerSet.Mode != WorkerSetAll {
		return false
	}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind: QueryExecAPOneCN, CurrentCN: intent.CurrentCN,
		CurrentCNPolicy: intent.Scheduling.CurrentCNPolicy, Intent: intent.Scheduling,
	})
	return decision.Satisfied && len(decision.Workers) == 1 && sameWorker(decision.Workers[0], intent.CurrentCN)
}
