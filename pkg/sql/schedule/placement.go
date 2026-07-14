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

import "sort"

const (
	ReasonLocalExecType            = "local-exec-type"
	ReasonMultiCN                  = "multi-cn"
	ReasonNoCandidateCN            = "no-candidate-cn"
	ReasonRequiredCurrentCN        = "required-current-cn"
	ReasonPreferredCurrentCN       = "preferred-current-cn"
	ReasonExcludedCurrentCN        = "excluded-current-cn"
	ReasonCurrentCNMissingIdentity = "current-cn-missing-identity"
	ReasonCurrentCNDraining        = "current-cn-draining"
	ReasonCurrentCNDrained         = "current-cn-drained"
	ReasonInvalidCurrentCNPolicy   = "invalid-current-cn-policy"
	ReasonDroppedUnroutableCN      = "unroutable-cn"
	ReasonDroppedDrainingCN        = "draining-cn"
	ReasonDroppedDrainedCN         = "drained-cn"
	ReasonDroppedDuplicateCN       = "duplicate-cn"
)

type QueryExecKind uint8

const (
	QueryExecTP QueryExecKind = iota
	QueryExecAPOneCN
	QueryExecAPMultiCN
)

func (k QueryExecKind) String() string {
	switch k {
	case QueryExecTP:
		return "tp"
	case QueryExecAPOneCN:
		return "ap-one-cn"
	case QueryExecAPMultiCN:
		return "ap-multi-cn"
	default:
		return "unknown"
	}
}

type CurrentCNPolicy uint8

const (
	CurrentCNAllowed CurrentCNPolicy = iota
	CurrentCNRequired
	CurrentCNPreferred
	CurrentCNExcluded
)

func (p CurrentCNPolicy) String() string {
	switch p {
	case CurrentCNAllowed:
		return "allowed"
	case CurrentCNRequired:
		return "required"
	case CurrentCNPreferred:
		return "preferred"
	case CurrentCNExcluded:
		return "excluded"
	default:
		return "unknown"
	}
}

type QueryRequest struct {
	ExecKind  QueryExecKind
	CurrentCN Worker
	// Candidates contains workers after candidate discovery and pool/label
	// resolution. Query placement only selects from this resolved set.
	Candidates          Workers
	CandidateResolution CandidateResolution
	CurrentCNPolicy     CurrentCNPolicy
}

type QueryDecision struct {
	ExecKind               QueryExecKind
	CurrentCN              Worker
	Workers                Workers
	Dropped                DroppedWorkers
	Reason                 string
	CandidateResolution    CandidateResolution
	ResolvedCandidateCount int
	CurrentCNPolicy        CurrentCNPolicy
	Satisfied              bool
}

type CandidateSource string

const (
	CandidateSourceUnspecified      CandidateSource = "unspecified"
	CandidateSourceNotRequired      CandidateSource = "not-required"
	CandidateSourceEngineNodes      CandidateSource = "engine-nodes"
	CandidateSourceClusterInventory CandidateSource = "cluster-inventory"
)

type PoolResolution string

const (
	PoolResolutionUnspecified       PoolResolution = "unspecified"
	PoolResolutionNotRequired       PoolResolution = "not-required"
	PoolResolutionLegacyEngineNodes PoolResolution = "legacy-engine-nodes"
	PoolResolutionTenantLabels      PoolResolution = "tenant-labels"
)

// CandidateResolution describes the boundary before worker selection. The
// legacy engine adapter currently performs discovery and pool/label filtering
// together; making that explicit prevents selection from depending directly on
// Engine.Nodes and allows the two stages to be separated later.
type CandidateResolution struct {
	DiscoverySource CandidateSource
	PoolResolution  PoolResolution
	DiscoveredCount int
}

func DecideQueryPlacement(req QueryRequest) QueryDecision {
	if !req.CurrentCNPolicy.Valid() {
		return queryDecision(req, nil, nil, ReasonInvalidCurrentCNPolicy, false)
	}
	if req.ExecKind == QueryExecTP || req.ExecKind == QueryExecAPOneCN {
		return decideLocalQueryPlacement(req)
	}

	workers, dropped := selectEligibleCandidateWorkers(req.Candidates)
	currentRejectReason, currentRejected := rejectedCurrentWorkerReason(req.CurrentCN)
	if currentRejected {
		workers = removeCurrentWorker(workers, req.CurrentCN)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && !hasWorkerIdentity(req.CurrentCN) {
		return queryDecision(req, workers, dropped, ReasonCurrentCNMissingIdentity, false)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && currentRejected {
		return queryDecision(req, workers, dropped, currentRejectReason, false)
	}
	if req.CurrentCNPolicy == CurrentCNExcluded {
		if !hasWorkerIdentity(req.CurrentCN) {
			return queryDecision(req, workers, dropped, ReasonCurrentCNMissingIdentity, false)
		}
		workers = removeCurrentWorker(workers, req.CurrentCN)
		if len(workers) == 0 {
			return queryDecision(req, workers, dropped, ReasonExcludedCurrentCN, false)
		}
		return queryDecision(req, workers, dropped, ReasonExcludedCurrentCN, true)
	}

	reason := ReasonMultiCN
	if len(workers) == 0 {
		if currentRejected {
			return queryDecision(req, workers, dropped, currentRejectReason, false)
		}
		workers = ensureCurrentWorker(workers, req.CurrentCN)
		reason = ReasonNoCandidateCN
		return queryDecision(req, workers, dropped, reason, len(workers) > 0)
	}

	switch req.CurrentCNPolicy {
	case CurrentCNRequired:
		workers = ensureCurrentWorker(workers, req.CurrentCN)
		reason = ReasonRequiredCurrentCN
	case CurrentCNPreferred:
		if !currentRejected {
			preferredWorkers, ok := preferCurrentWorker(workers, req.CurrentCN)
			if !ok {
				break
			}
			workers = preferredWorkers
			reason = ReasonPreferredCurrentCN
		}
	}
	return queryDecision(req, workers, dropped, reason, true)
}

func decideLocalQueryPlacement(req QueryRequest) QueryDecision {
	if req.CurrentCNPolicy == CurrentCNExcluded {
		return queryDecision(req, nil, nil, ReasonExcludedCurrentCN, false)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && !hasWorkerIdentity(req.CurrentCN) {
		return queryDecision(req, nil, nil, ReasonCurrentCNMissingIdentity, false)
	}
	workers := Workers{req.CurrentCN}
	return queryDecision(req, workers, nil, ReasonLocalExecType, true)
}

func (p CurrentCNPolicy) Valid() bool {
	switch p {
	case CurrentCNAllowed, CurrentCNRequired, CurrentCNPreferred, CurrentCNExcluded:
		return true
	default:
		return false
	}
}

func queryDecision(req QueryRequest, workers Workers, dropped DroppedWorkers, reason string, satisfied bool) QueryDecision {
	workers = orderDecisionWorkers(req, workers, reason)
	resolution := req.CandidateResolution
	if resolution.DiscoverySource == "" {
		resolution.DiscoverySource = CandidateSourceUnspecified
	}
	if resolution.PoolResolution == "" {
		resolution.PoolResolution = PoolResolutionUnspecified
	}
	resolution.DiscoveredCount = max(resolution.DiscoveredCount, 0)
	return QueryDecision{
		ExecKind:               req.ExecKind,
		CurrentCN:              req.CurrentCN,
		Workers:                workers,
		Dropped:                cloneDroppedWorkers(dropped),
		Reason:                 reason,
		CandidateResolution:    resolution,
		ResolvedCandidateCount: len(req.Candidates),
		CurrentCNPolicy:        req.CurrentCNPolicy,
		Satisfied:              satisfied,
	}
}

func orderDecisionWorkers(req QueryRequest, workers Workers, reason string) Workers {
	workers = cloneWorkers(workers)
	if req.ExecKind != QueryExecAPMultiCN || len(workers) < 2 {
		return workers
	}
	if reason == ReasonPreferredCurrentCN {
		sortWorkersByAddr(workers[1:])
		return workers
	}
	sortWorkersByAddr(workers)
	return workers
}

func sortWorkersByAddr(workers Workers) {
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].Addr < workers[j].Addr
	})
}

func ensureCurrentWorker(workers Workers, current Worker) Workers {
	if !hasWorkerIdentity(current) {
		return workers
	}
	for _, worker := range workers {
		if sameWorker(worker, current) {
			return workers
		}
	}
	return append(workers, current)
}

func removeCurrentWorker(workers Workers, current Worker) Workers {
	if !hasWorkerIdentity(current) {
		return workers
	}
	filtered := workers[:0]
	for _, worker := range workers {
		if sameWorker(worker, current) {
			continue
		}
		filtered = append(filtered, worker)
	}
	return filtered
}

func preferCurrentWorker(workers Workers, current Worker) (Workers, bool) {
	if !hasWorkerIdentity(current) {
		return workers, false
	}
	for idx, worker := range workers {
		if !sameWorker(worker, current) {
			continue
		}
		if !hasWorkerRoute(worker) {
			continue
		}
		if idx == 0 {
			return workers, true
		}
		preferred := make(Workers, 0, len(workers))
		preferred = append(preferred, worker)
		preferred = append(preferred, workers[:idx]...)
		preferred = append(preferred, workers[idx+1:]...)
		return preferred, true
	}
	return workers, false
}

func selectEligibleCandidateWorkers(workers Workers) (Workers, DroppedWorkers) {
	if len(workers) == 0 {
		return nil, nil
	}
	selected := make(Workers, 0, len(workers))
	var dropped DroppedWorkers
	for _, worker := range workers {
		if reason, ok := workerDropReason(worker); ok {
			dropped = append(dropped, DroppedWorker{Worker: worker, Reason: reason})
			continue
		}
		if containsWorker(selected, worker) {
			dropped = append(dropped, DroppedWorker{Worker: worker, Reason: ReasonDroppedDuplicateCN})
			continue
		}
		selected = append(selected, worker)
	}
	return selected, dropped
}

func workerDropReason(worker Worker) (string, bool) {
	switch worker.State {
	case WorkerStateDraining:
		return ReasonDroppedDrainingCN, true
	case WorkerStateDrained:
		return ReasonDroppedDrainedCN, true
	}
	if !hasWorkerRoute(worker) {
		return ReasonDroppedUnroutableCN, true
	}
	return "", false
}

func rejectedCurrentWorkerReason(worker Worker) (string, bool) {
	switch worker.State {
	case WorkerStateDraining:
		return ReasonCurrentCNDraining, true
	case WorkerStateDrained:
		return ReasonCurrentCNDrained, true
	default:
		return "", false
	}
}

func containsWorker(workers Workers, target Worker) bool {
	for _, worker := range workers {
		if sameWorker(worker, target) {
			return true
		}
	}
	return false
}

func hasWorkerIdentity(worker Worker) bool {
	return worker.ID != "" || worker.Addr != ""
}

func hasWorkerRoute(worker Worker) bool {
	return worker.Addr != ""
}

func sameWorker(worker Worker, current Worker) bool {
	if worker.ID != "" && current.ID != "" && worker.ID == current.ID {
		return true
	}
	if worker.Addr != "" && current.Addr != "" && worker.Addr == current.Addr {
		return true
	}
	return false
}
