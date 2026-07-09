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
	ReasonInvalidCurrentCNPolicy   = "invalid-current-cn-policy"
)

type QueryExecKind uint8

const (
	QueryExecTP QueryExecKind = iota
	QueryExecAPOneCN
	QueryExecAPMultiCN
)

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
	ExecKind        QueryExecKind
	CurrentCN       Worker
	Candidates      Workers
	CurrentCNPolicy CurrentCNPolicy
}

type QueryDecision struct {
	ExecKind        QueryExecKind
	Workers         Workers
	Reason          string
	CurrentCNPolicy CurrentCNPolicy
	Satisfied       bool
}

func DecideQueryPlacement(req QueryRequest) QueryDecision {
	if !req.CurrentCNPolicy.Valid() {
		return queryDecision(req, nil, ReasonInvalidCurrentCNPolicy, false)
	}
	if req.ExecKind == QueryExecTP || req.ExecKind == QueryExecAPOneCN {
		return decideLocalQueryPlacement(req)
	}

	workers := dedupeWorkers(req.Candidates)
	if req.CurrentCNPolicy == CurrentCNRequired && !hasWorkerIdentity(req.CurrentCN) {
		return queryDecision(req, workers, ReasonCurrentCNMissingIdentity, false)
	}
	if req.CurrentCNPolicy == CurrentCNExcluded {
		if !hasWorkerIdentity(req.CurrentCN) {
			return queryDecision(req, workers, ReasonCurrentCNMissingIdentity, false)
		}
		workers = removeCurrentWorker(workers, req.CurrentCN)
		if len(workers) == 0 {
			return queryDecision(req, workers, ReasonExcludedCurrentCN, false)
		}
		return queryDecision(req, workers, ReasonExcludedCurrentCN, true)
	}

	reason := ReasonMultiCN
	if len(workers) == 0 {
		workers = ensureCurrentWorker(workers, req.CurrentCN)
		reason = ReasonNoCandidateCN
		return queryDecision(req, workers, reason, len(workers) > 0)
	}

	switch req.CurrentCNPolicy {
	case CurrentCNRequired:
		workers = ensureCurrentWorker(workers, req.CurrentCN)
		reason = ReasonRequiredCurrentCN
	case CurrentCNPreferred:
		if preferredWorkers, ok := preferCurrentWorker(workers, req.CurrentCN); ok {
			workers = preferredWorkers
			reason = ReasonPreferredCurrentCN
		}
	}
	return queryDecision(req, workers, reason, true)
}

func decideLocalQueryPlacement(req QueryRequest) QueryDecision {
	if req.CurrentCNPolicy == CurrentCNExcluded {
		return queryDecision(req, nil, ReasonExcludedCurrentCN, false)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && !hasWorkerIdentity(req.CurrentCN) {
		return queryDecision(req, nil, ReasonCurrentCNMissingIdentity, false)
	}
	workers := Workers{req.CurrentCN}
	return queryDecision(req, workers, ReasonLocalExecType, true)
}

func (p CurrentCNPolicy) Valid() bool {
	switch p {
	case CurrentCNAllowed, CurrentCNRequired, CurrentCNPreferred, CurrentCNExcluded:
		return true
	default:
		return false
	}
}

func queryDecision(req QueryRequest, workers Workers, reason string, satisfied bool) QueryDecision {
	workers = orderDecisionWorkers(req, workers, reason)
	return QueryDecision{
		ExecKind:        req.ExecKind,
		Workers:         workers,
		Reason:          reason,
		CurrentCNPolicy: req.CurrentCNPolicy,
		Satisfied:       satisfied,
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

func dedupeWorkers(workers Workers) Workers {
	if len(workers) == 0 {
		return nil
	}
	deduped := make(Workers, 0, len(workers))
	for _, worker := range workers {
		if !hasWorkerRoute(worker) {
			continue
		}
		if containsWorker(deduped, worker) {
			continue
		}
		deduped = append(deduped, worker)
	}
	return deduped
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
