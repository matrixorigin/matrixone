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

import (
	"cmp"
	"slices"
)

const (
	ReasonLocalExecType               = "local-exec-type"
	ReasonMultiCN                     = "multi-cn"
	ReasonNoCandidateCN               = "no-candidate-cn"
	ReasonRequiredCurrentCN           = "required-current-cn"
	ReasonPreferredCurrentCN          = "preferred-current-cn"
	ReasonExcludedCurrentCN           = "excluded-current-cn"
	ReasonCurrentCNMissingIdentity    = "current-cn-missing-identity"
	ReasonCurrentCNDraining           = "current-cn-draining"
	ReasonCurrentCNDrained            = "current-cn-drained"
	ReasonInvalidCurrentCNPolicy      = "invalid-current-cn-policy"
	ReasonInvalidWorkerSetPolicy      = "invalid-worker-set-policy"
	ReasonInvalidSchedulingIntent     = "invalid-scheduling-intent"
	ReasonMissingSelectionKey         = "missing-selection-key"
	ReasonUnsupportedSchedulingIntent = "unsupported-scheduling-intent"
	ReasonStrictPoolFallback          = "strict-pool-fallback"
	ReasonRequiredCurrentOutsidePool  = "required-current-cn-outside-pool"
	ReasonDroppedUnroutableCN         = "unroutable-cn"
	ReasonDroppedDrainingCN           = "draining-cn"
	ReasonDroppedDrainedCN            = "drained-cn"
	ReasonDroppedDuplicateCN          = "duplicate-cn"
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
	Candidates           Workers
	CandidateResolution  CandidateResolution
	Intent               SchedulingIntent
	ResolvedPool         ResolvedPool
	CurrentCNPolicy      CurrentCNPolicy
	CurrentCNOrdinalZero bool
}

type QueryDecision struct {
	ExecKind               QueryExecKind
	CurrentCN              Worker
	Workers                Workers
	Dropped                DroppedWorkers
	Reason                 string
	CandidateResolution    CandidateResolution
	Intent                 SchedulingIntent
	ResolvedPool           ResolvedPoolDecision
	EligibleCount          int
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

type PoolFallbackPolicy uint8

const (
	PoolFallbackLegacyCompatible PoolFallbackPolicy = iota
	PoolFallbackStrict
)

func (p PoolFallbackPolicy) String() string {
	switch p {
	case PoolFallbackLegacyCompatible:
		return "legacy-compatible"
	case PoolFallbackStrict:
		return "strict"
	default:
		return "invalid"
	}
}

func (p PoolFallbackPolicy) Valid() bool {
	return p == PoolFallbackLegacyCompatible || p == PoolFallbackStrict
}

type EmptyWorkerPolicy uint8

const (
	EmptyWorkerLocalFallback EmptyWorkerPolicy = iota
	EmptyWorkerFail
)

func (p EmptyWorkerPolicy) String() string {
	switch p {
	case EmptyWorkerLocalFallback:
		return "local-fallback"
	case EmptyWorkerFail:
		return "fail"
	default:
		return "invalid"
	}
}

func (p EmptyWorkerPolicy) Valid() bool {
	return p == EmptyWorkerLocalFallback || p == EmptyWorkerFail
}

type WorkerSetMode uint8

const (
	WorkerSetAll WorkerSetMode = iota
	WorkerSetMax
)

func (m WorkerSetMode) String() string {
	switch m {
	case WorkerSetAll:
		return "all"
	case WorkerSetMax:
		return "max-workers"
	default:
		return "invalid"
	}
}

const WorkerSelectionAlgorithmV1 = "hrw-v1"

type WorkerSetPolicy struct {
	Mode             WorkerSetMode
	MaxWorkers       int
	SelectionKey     string
	AlgorithmVersion string
}

type SchedulingIntent struct {
	Explicit          bool
	RequestedPool     string
	PoolFallback      PoolFallbackPolicy
	EmptyWorkerPolicy EmptyWorkerPolicy
	CurrentCNPolicy   CurrentCNPolicy
	WorkerSet         WorkerSetPolicy
}

// ResolvedPool is immutable input to worker selection. Pool resolution owns
// fallback; selection may filter or rank this set but must never widen it.
type ResolvedPool struct {
	RequestedIdentity string
	Identity          string
	Resolution        PoolResolution
	Fallback          bool
	FallbackReason    string
	Workers           Workers
}

type ResolvedPoolDecision struct {
	RequestedIdentity string
	Identity          string
	Resolution        PoolResolution
	Fallback          bool
	FallbackReason    string
}

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
	if !req.Intent.PoolFallback.Valid() || !req.Intent.EmptyWorkerPolicy.Valid() {
		return queryDecision(req, nil, nil, ReasonInvalidSchedulingIntent, false)
	}
	if reason := validateWorkerSetPolicy(req.Intent.WorkerSet, false); reason != "" {
		return queryDecision(req, nil, nil, reason, false)
	}
	if req.Intent.PoolFallback == PoolFallbackStrict && req.ResolvedPool.Fallback {
		return queryDecision(req, nil, nil, ReasonStrictPoolFallback, false)
	}
	if req.ExecKind == QueryExecTP || req.ExecKind == QueryExecAPOneCN {
		// A max-worker policy is an upper bound, so a local query using one
		// worker satisfies every positive cap. Strict pool intent has already
		// rejected a compatibility fallback above. Treating either policy as
		// unsupported here makes the session unusable after SET because even
		// local control statements pass through query placement.
		return decideLocalQueryPlacement(req)
	}

	resolved := resolvedWorkers(req)
	workers, dropped := selectEligibleCandidateWorkers(resolved)
	currentRejectReason, currentRejected := rejectedCurrentWorkerReason(req.CurrentCN)
	if currentRejected {
		workers = removeCurrentWorker(workers, req.CurrentCN)
	}
	eligibleCount := len(workers)
	makeDecision := func(decisionWorkers Workers, reason string, satisfied bool) QueryDecision {
		decision := queryDecision(req, decisionWorkers, dropped, reason, satisfied)
		decision.EligibleCount = eligibleCount
		return decision
	}
	if req.CurrentCNPolicy == CurrentCNRequired && !hasWorkerIdentity(req.CurrentCN) {
		return makeDecision(workers, ReasonCurrentCNMissingIdentity, false)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && currentRejected {
		return makeDecision(workers, currentRejectReason, false)
	}
	if req.CurrentCNPolicy == CurrentCNExcluded {
		if !hasWorkerIdentity(req.CurrentCN) {
			return makeDecision(workers, ReasonCurrentCNMissingIdentity, false)
		}
		workers = removeCurrentWorker(workers, req.CurrentCN)
		eligibleCount = len(workers)
		if len(workers) == 0 {
			return makeDecision(workers, ReasonExcludedCurrentCN, false)
		}
		selected, reason, ok := selectWorkerSubset(req.Intent.WorkerSet, workers, nil)
		if !ok {
			return makeDecision(nil, reason, false)
		}
		return makeDecision(selected, ReasonExcludedCurrentCN, true)
	}

	reason := ReasonMultiCN
	if len(workers) == 0 {
		if currentRejected {
			return makeDecision(workers, currentRejectReason, false)
		}
		if req.Intent.EmptyWorkerPolicy == EmptyWorkerFail {
			return makeDecision(nil, ReasonNoCandidateCN, false)
		}
		workers = ensureCurrentWorker(workers, req.CurrentCN)
		reason = ReasonNoCandidateCN
		return makeDecision(workers, reason, len(workers) > 0)
	}

	switch req.CurrentCNPolicy {
	case CurrentCNRequired:
		if !containsWorker(workers, req.CurrentCN) {
			return makeDecision(nil, ReasonRequiredCurrentOutsidePool, false)
		}
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
	var pinned *Worker
	if req.CurrentCNPolicy == CurrentCNRequired {
		pinned = &req.CurrentCN
	}
	selected, selectionReason, ok := selectWorkerSubset(req.Intent.WorkerSet, workers, pinned)
	if !ok {
		return makeDecision(nil, selectionReason, false)
	}
	if req.CurrentCNPolicy == CurrentCNRequired && !containsWorker(selected, req.CurrentCN) {
		return makeDecision(nil, ReasonRequiredCurrentOutsidePool, false)
	}
	return makeDecision(selected, reason, true)
}

func resolvedWorkers(req QueryRequest) Workers {
	if req.ResolvedPool.Workers != nil {
		return req.ResolvedPool.Workers
	}
	return req.Candidates
}

func selectWorkerSubset(policy WorkerSetPolicy, workers Workers, pinned *Worker) (Workers, string, bool) {
	if reason := validateWorkerSetPolicy(policy, true); reason != "" {
		return nil, reason, false
	}
	switch policy.Mode {
	case WorkerSetAll:
		return workers, "", true
	case WorkerSetMax:
		if policy.MaxWorkers >= len(workers) {
			return workers, "", true
		}
		var pinnedWorker Worker
		if pinned != nil {
			for _, worker := range workers {
				if sameWorker(worker, *pinned) {
					pinnedWorker = worker
					break
				}
			}
		}
		type rankedWorker struct {
			index int
			score uint64
		}
		ranked := make([]rankedWorker, 0, len(workers))
		for i := range workers {
			if pinned != nil && sameWorker(workers[i], *pinned) {
				continue
			}
			ranked = append(ranked, rankedWorker{
				index: i,
				score: stableHRWWorkerScore(policy.SelectionKey, workers[i]),
			})
		}
		slices.SortFunc(ranked, func(a, b rankedWorker) int {
			if n := cmp.Compare(b.score, a.score); n != 0 {
				return n
			}
			return compareWorkerIdentity(workers[a.index], workers[b.index])
		})
		selected := make(Workers, 0, policy.MaxWorkers)
		if pinned != nil {
			selected = append(selected, pinnedWorker)
		}
		for i := 0; len(selected) < policy.MaxWorkers; i++ {
			selected = append(selected, workers[ranked[i].index])
		}
		return selected, "", true
	default:
		return nil, ReasonInvalidWorkerSetPolicy, false
	}
}

func validateWorkerSetPolicy(policy WorkerSetPolicy, requireSelectionKey bool) string {
	switch policy.Mode {
	case WorkerSetAll:
		if policy.MaxWorkers != 0 || policy.SelectionKey != "" || policy.AlgorithmVersion != "" {
			return ReasonInvalidWorkerSetPolicy
		}
	case WorkerSetMax:
		if policy.MaxWorkers <= 0 {
			return ReasonInvalidWorkerSetPolicy
		}
		if requireSelectionKey && policy.SelectionKey == "" {
			return ReasonMissingSelectionKey
		}
		if policy.AlgorithmVersion != "" && policy.AlgorithmVersion != WorkerSelectionAlgorithmV1 {
			return ReasonInvalidWorkerSetPolicy
		}
	default:
		return ReasonInvalidWorkerSetPolicy
	}
	return ""
}

// stableHRWScore is FNV-1a with length-delimited fields. Its byte-level
// definition is deliberately local and versioned so Go hash seed changes can
// never reshuffle a statement retry.
func stableHRWScore(key, identity string) uint64 {
	h := stableFNV64(14695981039346656037)
	h.writeField(WorkerSelectionAlgorithmV1)
	h.writeField(key)
	h.writeField(identity)
	return uint64(h)
}

func stableHRWWorkerScore(key string, worker Worker) uint64 {
	h := stableFNV64(14695981039346656037)
	h.writeField(WorkerSelectionAlgorithmV1)
	h.writeField(key)
	if worker.ID != "" {
		h.writeCompositeField("id:", worker.ID)
	} else {
		h.writeCompositeField("addr:", worker.Addr)
	}
	return uint64(h)
}

type stableFNV64 uint64

func (h *stableFNV64) writeField(value string) {
	h.writeLength(len(value))
	h.writeBytes(value)
}

func (h *stableFNV64) writeCompositeField(prefix, value string) {
	h.writeLength(len(prefix) + len(value))
	h.writeBytes(prefix)
	h.writeBytes(value)
}

func (h *stableFNV64) writeLength(value int) {
	length := uint64(value)
	for i := 0; i < 8; i++ {
		h.writeByte(byte(length))
		length >>= 8
	}
}

func (h *stableFNV64) writeBytes(value string) {
	for i := 0; i < len(value); i++ {
		h.writeByte(value[i])
	}
}

func (h *stableFNV64) writeByte(value byte) {
	*h ^= stableFNV64(value)
	*h *= 1099511628211
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
		Intent:                 req.Intent,
		ResolvedPool:           resolvedPoolDecision(req),
		ResolvedCandidateCount: len(resolvedWorkers(req)),
		CurrentCNPolicy:        req.CurrentCNPolicy,
		Satisfied:              satisfied,
	}
}

func orderDecisionWorkers(req QueryRequest, workers Workers, reason string) Workers {
	if req.ExecKind != QueryExecAPMultiCN || len(workers) < 2 {
		return workers
	}
	if reason == ReasonPreferredCurrentCN || (reason == ReasonRequiredCurrentCN && req.CurrentCNOrdinalZero) {
		if currentFirst, ok := preferCurrentWorker(workers, req.CurrentCN); ok {
			sortWorkersByAddr(currentFirst[1:])
			return currentFirst
		}
	}
	sortWorkersByAddr(workers)
	return workers
}

func sortWorkersByAddr(workers Workers) {
	slices.SortFunc(workers, func(a, b Worker) int {
		if n := compareWorkerIdentity(a, b); n != 0 {
			return n
		}
		return cmp.Compare(a.Addr, b.Addr)
	})
}

func compareWorkerIdentity(a, b Worker) int {
	switch {
	case a.ID == "" && b.ID != "":
		return -1
	case a.ID != "" && b.ID == "":
		return 1
	case a.ID != "":
		return cmp.Compare(a.ID, b.ID)
	default:
		return cmp.Compare(a.Addr, b.Addr)
	}
}

func resolvedPoolDecision(req QueryRequest) ResolvedPoolDecision {
	pool := req.ResolvedPool
	if pool.Resolution == "" {
		pool.Resolution = req.CandidateResolution.PoolResolution
	}
	return ResolvedPoolDecision{
		RequestedIdentity: pool.RequestedIdentity,
		Identity:          pool.Identity,
		Resolution:        pool.Resolution,
		Fallback:          pool.Fallback,
		FallbackReason:    pool.FallbackReason,
	}
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
	var seenIDs, seenAddrs map[string]struct{}
	if len(workers) >= 16 {
		seenIDs = make(map[string]struct{}, len(workers))
		seenAddrs = make(map[string]struct{}, len(workers))
	}
	var dropped DroppedWorkers
	for _, worker := range workers {
		if reason, ok := workerDropReason(worker); ok {
			dropped = append(dropped, DroppedWorker{Worker: worker, Reason: reason})
			continue
		}
		duplicate := false
		if seenIDs == nil {
			duplicate = containsWorker(selected, worker)
		} else {
			_, duplicateID := seenIDs[worker.ID]
			_, duplicateAddr := seenAddrs[worker.Addr]
			duplicate = (worker.ID != "" && duplicateID) || (worker.Addr != "" && duplicateAddr)
		}
		if duplicate {
			dropped = append(dropped, DroppedWorker{Worker: worker, Reason: ReasonDroppedDuplicateCN})
			continue
		}
		selected = append(selected, worker)
		if seenIDs != nil {
			if worker.ID != "" {
				seenIDs[worker.ID] = struct{}{}
			}
			if worker.Addr != "" {
				seenAddrs[worker.Addr] = struct{}{}
			}
		}
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
	if !hasWorkerIdentity(worker) {
		return ReasonDroppedUnroutableCN, true
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
	switch worker.Route {
	case WorkerRouteLocal:
		return true
	case WorkerRouteRemote:
		return worker.Addr != ""
	case WorkerRouteUnknown:
		return worker.Addr != ""
	default:
		return false
	}
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
