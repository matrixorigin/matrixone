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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	workloadPolicyVersion       = 1
	maxWorkloadPolicyBytes      = 64 << 10
	maxWorkloadPolicyLabels     = 16
	maxWorkloadPolicyLabelPart  = 128
	maxWorkloadPolicyPoolLength = 256
)

type WorkloadClass string

const (
	WorkloadUnclassified WorkloadClass = "unclassified"
	WorkloadTP           WorkloadClass = "tp"
	WorkloadAP           WorkloadClass = "ap"
	WorkloadLoad         WorkloadClass = "load"
	WorkloadMaintenance  WorkloadClass = "maintenance"
	WorkloadInternal     WorkloadClass = "internal"
)

func (c WorkloadClass) Valid() bool {
	switch c {
	case WorkloadUnclassified,
		WorkloadTP,
		WorkloadAP,
		WorkloadLoad,
		WorkloadMaintenance,
		WorkloadInternal:
		return true
	default:
		return false
	}
}

type WorkloadPolicySource string

const (
	WorkloadPolicySourceLegacy        WorkloadPolicySource = "legacy"
	WorkloadPolicySourceAccountGlobal WorkloadPolicySource = "account-global"
)

type WorkloadRoutingMode string

const (
	WorkloadRoutingLocal  WorkloadRoutingMode = "local"
	WorkloadRoutingSingle WorkloadRoutingMode = "single-worker"
	WorkloadRoutingMulti  WorkloadRoutingMode = "multi-worker"
)

type WorkloadDescriptor struct {
	Class            WorkloadClass
	ExecKind         QueryExecKind
	Internal         bool
	Tenant           string
	IngressLabels    map[string]string
	LegacyPool       string
	SchedulingIntent SchedulingIntent
}

type WorkloadPoolConstraint struct {
	Identity string
	Labels   map[string]string
}

type WorkloadPolicyRule struct {
	PoolIdentity      string
	Labels            map[string]string
	PoolFallback      PoolFallbackPolicy
	EmptyWorkerPolicy EmptyWorkerPolicy
	CurrentCNPolicy   CurrentCNPolicy
	WorkerSet         WorkerSetPolicy
}

// WorkloadPolicySet is an immutable statement snapshot. Callers must obtain it
// through ParseWorkloadPolicyConfig or Clone so its maps are never shared with
// a mutable configuration owner.
type WorkloadPolicySet struct {
	Generation    string
	Rules         map[WorkloadClass]WorkloadPolicyRule
	InvalidReason string
}

func (p WorkloadPolicySet) Configured() bool {
	return len(p.Rules) > 0
}

func (p WorkloadPolicySet) Clone() WorkloadPolicySet {
	cloned := WorkloadPolicySet{
		Generation:    p.Generation,
		InvalidReason: p.InvalidReason,
	}
	if len(p.Rules) == 0 {
		return cloned
	}
	cloned.Rules = make(map[WorkloadClass]WorkloadPolicyRule, len(p.Rules))
	for class, rule := range p.Rules {
		rule.Labels = cloneWorkloadLabels(rule.Labels)
		cloned.Rules[class] = rule
	}
	return cloned
}

type EffectiveWorkloadPolicy struct {
	WorkloadClass WorkloadClass
	Source        WorkloadPolicySource
	Generation    string
	Reason        string
	Applied       bool
	Routing       WorkloadRoutingMode
	Pool          WorkloadPoolConstraint
	Intent        SchedulingIntent
}

func (p EffectiveWorkloadPolicy) RequiresPoolResolution() bool {
	return p.Applied
}

type workloadPolicyDocument struct {
	Version  int                           `json:"version"`
	Policies map[string]workloadPolicyJSON `json:"policies"`
}

type workloadPolicyJSON struct {
	Pool        string            `json:"pool"`
	Labels      map[string]string `json:"labels"`
	Fallback    string            `json:"fallback,omitempty"`
	EmptyWorker string            `json:"empty_worker,omitempty"`
	CurrentCN   string            `json:"current_cn,omitempty"`
	MaxWorkers  int               `json:"max_workers,omitempty"`
}

// ParseWorkloadPolicyConfig parses the account-level, administrator-owned
// workload policy. An empty value deliberately means the historical scheduler
// behavior. JSON decoding is strict so a misspelled safety field cannot be
// silently ignored.
func ParseWorkloadPolicyConfig(raw string) (WorkloadPolicySet, error) {
	if strings.TrimSpace(raw) == "" {
		return WorkloadPolicySet{}, nil
	}
	if len(raw) > maxWorkloadPolicyBytes {
		return WorkloadPolicySet{}, fmt.Errorf(
			"query workload policy exceeds %d bytes", maxWorkloadPolicyBytes)
	}
	if err := rejectDuplicateWorkloadPolicyKeys(raw); err != nil {
		return WorkloadPolicySet{}, err
	}

	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var document workloadPolicyDocument
	if err := decoder.Decode(&document); err != nil {
		return WorkloadPolicySet{}, fmt.Errorf("invalid query workload policy: %w", err)
	}
	if err := requireWorkloadPolicyEOF(decoder); err != nil {
		return WorkloadPolicySet{}, err
	}
	if document.Version != workloadPolicyVersion {
		return WorkloadPolicySet{}, fmt.Errorf(
			"unsupported query workload policy version %d", document.Version)
	}
	if len(document.Policies) == 0 {
		return WorkloadPolicySet{}, fmt.Errorf("query workload policy has no policies")
	}

	set := WorkloadPolicySet{
		Rules: make(map[WorkloadClass]WorkloadPolicyRule, len(document.Policies)),
	}
	sum := sha256.Sum256([]byte(raw))
	set.Generation = hex.EncodeToString(sum[:16])
	for name, configured := range document.Policies {
		class := WorkloadClass(strings.ToLower(strings.TrimSpace(name)))
		if !class.Valid() {
			return WorkloadPolicySet{}, fmt.Errorf(
				"invalid query workload class %q", name)
		}
		if _, exists := set.Rules[class]; exists {
			return WorkloadPolicySet{}, fmt.Errorf(
				"duplicate query workload class %q", class)
		}
		rule, err := configured.toRule(class)
		if err != nil {
			return WorkloadPolicySet{}, err
		}
		set.Rules[class] = rule
	}
	return set, nil
}

func rejectDuplicateWorkloadPolicyKeys(raw string) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	var consumeValue func() error
	consumeValue = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				token, err = decoder.Token()
				if err != nil {
					return err
				}
				key, ok := token.(string)
				if !ok {
					return fmt.Errorf("object key is not a string")
				}
				if _, exists := keys[key]; exists {
					return fmt.Errorf("duplicate JSON field %q", key)
				}
				keys[key] = struct{}{}
				if err = consumeValue(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err = consumeValue(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}
	if err := consumeValue(); err != nil {
		return fmt.Errorf("invalid query workload policy: %w", err)
	}
	return nil
}

func requireWorkloadPolicyEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("invalid query workload policy: %w", err)
	}
	return fmt.Errorf("invalid query workload policy: multiple JSON values")
}

func (configured workloadPolicyJSON) toRule(
	class WorkloadClass,
) (WorkloadPolicyRule, error) {
	pool := strings.TrimSpace(configured.Pool)
	if pool == "" || len(pool) > maxWorkloadPolicyPoolLength {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q has invalid pool identity", class)
	}
	if len(configured.Labels) == 0 ||
		len(configured.Labels) > maxWorkloadPolicyLabels {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q must have 1..%d labels",
			class,
			maxWorkloadPolicyLabels)
	}
	labels := make(map[string]string, len(configured.Labels))
	for key, value := range configured.Labels {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" ||
			len(key) > maxWorkloadPolicyLabelPart ||
			len(value) > maxWorkloadPolicyLabelPart {
			return WorkloadPolicyRule{}, fmt.Errorf(
				"query workload policy %q has invalid target label", class)
		}
		if strings.EqualFold(key, "account") {
			return WorkloadPolicyRule{}, fmt.Errorf(
				"query workload policy %q cannot set protected account label", class)
		}
		if _, exists := labels[key]; exists {
			return WorkloadPolicyRule{}, fmt.Errorf(
				"query workload policy %q has duplicate target label %q", class, key)
		}
		labels[key] = value
	}

	fallback, err := parseWorkloadPoolFallback(configured.Fallback)
	if err != nil {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q: %w", class, err)
	}
	emptyWorker, err := parseWorkloadEmptyWorker(configured.EmptyWorker, fallback)
	if err != nil {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q: %w", class, err)
	}
	currentCN, err := parseWorkloadCurrentCN(configured.CurrentCN)
	if err != nil {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q: %w", class, err)
	}
	if class == WorkloadTP && currentCN != CurrentCNRequired {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q must require the current CN", class)
	}
	if configured.MaxWorkers < 0 {
		return WorkloadPolicyRule{}, fmt.Errorf(
			"query workload policy %q has negative max_workers", class)
	}
	workerSet := WorkerSetPolicy{Mode: WorkerSetAll}
	if configured.MaxWorkers > 0 {
		workerSet.Mode = WorkerSetMax
		workerSet.MaxWorkers = configured.MaxWorkers
	}
	return WorkloadPolicyRule{
		PoolIdentity:      pool,
		Labels:            labels,
		PoolFallback:      fallback,
		EmptyWorkerPolicy: emptyWorker,
		CurrentCNPolicy:   currentCN,
		WorkerSet:         workerSet,
	}, nil
}

func parseWorkloadPoolFallback(value string) (PoolFallbackPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "strict":
		return PoolFallbackStrict, nil
	case "legacy-compatible":
		return PoolFallbackLegacyCompatible, nil
	default:
		return PoolFallbackPolicy(255), fmt.Errorf("invalid fallback %q", value)
	}
}

func parseWorkloadEmptyWorker(
	value string,
	fallback PoolFallbackPolicy,
) (EmptyWorkerPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		if fallback == PoolFallbackStrict {
			return EmptyWorkerFail, nil
		}
		return EmptyWorkerLocalFallback, nil
	case "fail":
		return EmptyWorkerFail, nil
	case "local-fallback":
		if fallback == PoolFallbackStrict {
			return EmptyWorkerPolicy(255), fmt.Errorf(
				"strict fallback cannot use local-fallback for empty workers")
		}
		return EmptyWorkerLocalFallback, nil
	default:
		return EmptyWorkerPolicy(255), fmt.Errorf(
			"invalid empty_worker %q", value)
	}
}

func parseWorkloadCurrentCN(value string) (CurrentCNPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "allowed":
		return CurrentCNAllowed, nil
	case "required":
		return CurrentCNRequired, nil
	case "preferred":
		return CurrentCNPreferred, nil
	case "excluded":
		return CurrentCNExcluded, nil
	default:
		return CurrentCNPolicy(255), fmt.Errorf("invalid current_cn %q", value)
	}
}

// ResolveWorkloadPolicy maps one immutable descriptor to the policy consumed
// by candidate resolution and placement. It never reads candidate or load
// state. Account is injected after parsing and therefore cannot be replaced by
// administrator policy or statement/session input.
func ResolveWorkloadPolicy(
	descriptor WorkloadDescriptor,
	set WorkloadPolicySet,
) EffectiveWorkloadPolicy {
	class := descriptor.Class
	if descriptor.Internal {
		class = WorkloadInternal
	}
	if !class.Valid() {
		class = WorkloadUnclassified
	}
	legacyIntent := descriptor.SchedulingIntent
	if legacyIntent.RequestedPool == "" {
		legacyIntent.RequestedPool = descriptor.LegacyPool
	}
	legacy := EffectiveWorkloadPolicy{
		WorkloadClass: class,
		Source:        WorkloadPolicySourceLegacy,
		Reason:        "no-matching-workload-policy",
		Routing:       legacyWorkloadRouting(descriptor.ExecKind),
		Pool: WorkloadPoolConstraint{
			Identity: legacyIntent.RequestedPool,
			Labels:   cloneWorkloadLabels(descriptor.IngressLabels),
		},
		Intent: legacyIntent,
	}
	if set.InvalidReason != "" {
		legacy.Source = WorkloadPolicySourceAccountGlobal
		legacy.Generation = set.Generation
		legacy.Reason = "invalid-workload-policy"
		legacy.Intent.Explicit = true
		legacy.Intent.PoolFallback = PoolFallbackPolicy(255)
		return legacy
	}
	rule, ok := set.Rules[class]
	if !ok {
		if !set.Configured() {
			legacy.Reason = "workload-policy-not-configured"
		}
		return legacy
	}
	if ValidateSchedulingIntent(legacyIntent) != "" {
		legacy.Source = WorkloadPolicySourceAccountGlobal
		legacy.Generation = set.Generation
		legacy.Reason = "invalid-user-scheduling-intent"
		return legacy
	}

	tenant := strings.TrimSpace(descriptor.Tenant)
	if tenant == "" {
		legacy.Source = WorkloadPolicySourceAccountGlobal
		legacy.Generation = set.Generation
		legacy.Reason = "workload-policy-missing-tenant"
		legacy.Intent.Explicit = true
		legacy.Intent.PoolFallback = PoolFallbackPolicy(255)
		return legacy
	}
	targetLabels := cloneWorkloadLabels(rule.Labels)
	targetLabels["account"] = tenant
	intent := legacyIntent
	intent.Explicit = true
	intent.RequestedPool = rule.PoolIdentity
	intent.CurrentCNPolicy = rule.CurrentCNPolicy
	intent.EmptyWorkerPolicy = rule.EmptyWorkerPolicy
	intent.PoolFallback = rule.PoolFallback
	if legacyIntent.PoolFallback == PoolFallbackStrict {
		// Session and statement intent may strengthen an administrator policy,
		// but cannot relax it.
		intent.PoolFallback = PoolFallbackStrict
		intent.EmptyWorkerPolicy = EmptyWorkerFail
	}
	intent.WorkerSet = mergeWorkloadWorkerSet(rule.WorkerSet, legacyIntent.WorkerSet)

	routing := WorkloadRoutingSingle
	switch {
	case class == WorkloadTP:
		routing = WorkloadRoutingLocal
		intent.WorkerSet = capWorkloadWorkerSet(intent.WorkerSet, 1)
	case descriptor.ExecKind == QueryExecAPMultiCN:
		routing = WorkloadRoutingMulti
	default:
		intent.WorkerSet = capWorkloadWorkerSet(intent.WorkerSet, 1)
	}
	return EffectiveWorkloadPolicy{
		WorkloadClass: class,
		Source:        WorkloadPolicySourceAccountGlobal,
		Generation:    set.Generation,
		Reason:        "matched-account-workload-policy",
		Applied:       true,
		Routing:       routing,
		Pool: WorkloadPoolConstraint{
			Identity: rule.PoolIdentity,
			Labels:   targetLabels,
		},
		Intent: intent,
	}
}

func legacyWorkloadRouting(execKind QueryExecKind) WorkloadRoutingMode {
	if execKind == QueryExecAPMultiCN {
		return WorkloadRoutingMulti
	}
	return WorkloadRoutingLocal
}

func mergeWorkloadWorkerSet(
	operator WorkerSetPolicy,
	request WorkerSetPolicy,
) WorkerSetPolicy {
	if request.Mode != WorkerSetMax {
		return operator
	}
	if operator.Mode != WorkerSetMax || request.MaxWorkers < operator.MaxWorkers {
		return request
	}
	return operator
}

func capWorkloadWorkerSet(policy WorkerSetPolicy, maximum int) WorkerSetPolicy {
	if policy.Mode == WorkerSetMax && policy.MaxWorkers <= maximum {
		return policy
	}
	return WorkerSetPolicy{
		Mode:       WorkerSetMax,
		MaxWorkers: maximum,
	}
}

func cloneWorkloadLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}
