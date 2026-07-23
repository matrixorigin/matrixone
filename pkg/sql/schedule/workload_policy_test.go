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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseWorkloadPolicyConfig(t *testing.T) {
	set, err := ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"tp": {
				"pool": "tp",
				"labels": {"role": "tp"},
				"current_cn": "required"
			},
			"ap": {
				"pool": "ap",
				"labels": {"role": "ap"},
				"current_cn": "excluded",
				"max_workers": 4
			},
			"load": {
				"pool": "etl",
				"labels": {"role": "etl"},
				"fallback": "legacy-compatible",
				"empty_worker": "local-fallback"
			}
		}
	}`)
	require.NoError(t, err)
	require.Len(t, set.Generation, 32)
	require.Len(t, set.Rules, 3)
	require.Equal(t, CurrentCNRequired, set.Rules[WorkloadTP].CurrentCNPolicy)
	require.Equal(t, WorkerSetMax, set.Rules[WorkloadAP].WorkerSet.Mode)
	require.Equal(t, 4, set.Rules[WorkloadAP].WorkerSet.MaxWorkers)
	require.Equal(t, PoolFallbackLegacyCompatible, set.Rules[WorkloadLoad].PoolFallback)
	require.Equal(t, EmptyWorkerLocalFallback, set.Rules[WorkloadLoad].EmptyWorkerPolicy)

	cloned := set.Clone()
	clonedRule := cloned.Rules[WorkloadAP]
	clonedRule.Labels["role"] = "changed"
	cloned.Rules[WorkloadAP] = clonedRule
	require.Equal(t, "ap", set.Rules[WorkloadAP].Labels["role"])
}

func TestParseWorkloadPolicyConfigRejectsUnsafeOrInvalidPolicy(t *testing.T) {
	_, err := ParseWorkloadPolicyConfig(strings.Repeat("x", maxWorkloadPolicyBytes+1))
	require.ErrorContains(t, err, "exceeds")

	tests := []struct {
		name   string
		policy string
		reason string
	}{
		{
			name:   "unknown version",
			policy: `{"version":2,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"}}}}`,
			reason: "unsupported",
		},
		{
			name:   "unknown field",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"},"strcit":true}}}`,
			reason: "unknown field",
		},
		{
			name:   "protected account",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"account":"other"}}}}`,
			reason: "protected account",
		},
		{
			name:   "normalized duplicate class",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"}},"AP":{"pool":"other","labels":{"role":"other"}}}}`,
			reason: "duplicate",
		},
		{
			name:   "duplicate JSON field",
			policy: `{"version":1,"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"}}}}`,
			reason: "duplicate JSON field",
		},
		{
			name:   "normalized duplicate label",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"," role":"other"}}}}`,
			reason: "duplicate target label",
		},
		{
			name:   "tp relocation",
			policy: `{"version":1,"policies":{"tp":{"pool":"tp","labels":{"role":"tp"},"current_cn":"allowed"}}}`,
			reason: "must require",
		},
		{
			name:   "strict local fallback",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"},"empty_worker":"local-fallback"}}}`,
			reason: "strict fallback",
		},
		{
			name:   "trailing value",
			policy: `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"}}}} {}`,
			reason: "multiple JSON values",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseWorkloadPolicyConfig(test.policy)
			require.ErrorContains(t, err, test.reason)
		})
	}
}

func TestResolveWorkloadPolicySeparatesIngressAndTargetPool(t *testing.T) {
	set, err := ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"},
				"current_cn": "excluded",
				"max_workers": 4
			}
		}
	}`)
	require.NoError(t, err)
	ingress := map[string]string{"account": "tenant-a", "role": "tp"}
	policy := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:         WorkloadAP,
		ExecKind:      QueryExecAPMultiCN,
		Tenant:        "tenant-a",
		IngressLabels: ingress,
		LegacyPool:    "legacy-ingress",
		SchedulingIntent: SchedulingIntent{
			PoolFallback:      PoolFallbackLegacyCompatible,
			EmptyWorkerPolicy: EmptyWorkerLocalFallback,
			CurrentCNPolicy:   CurrentCNAllowed,
			WorkerSet: WorkerSetPolicy{
				Mode:       WorkerSetMax,
				MaxWorkers: 2,
			},
		},
	}, set)

	require.True(t, policy.Applied)
	require.Equal(t, WorkloadPolicySourceAccountGlobal, policy.Source)
	require.Equal(t, WorkloadRoutingMulti, policy.Routing)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "ap",
	}, policy.Pool.Labels)
	require.Equal(t, "tenant-ap", policy.Intent.RequestedPool)
	require.Equal(t, PoolFallbackStrict, policy.Intent.PoolFallback)
	require.Equal(t, EmptyWorkerFail, policy.Intent.EmptyWorkerPolicy)
	require.Equal(t, CurrentCNExcluded, policy.Intent.CurrentCNPolicy)
	require.Equal(t, 2, policy.Intent.WorkerSet.MaxWorkers)
	require.Equal(t, "tp", ingress["role"])
}

func TestResolveWorkloadPolicyRoutingAndLegacyCompatibility(t *testing.T) {
	set, err := ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"tp": {
				"pool": "tp",
				"labels": {"role": "tp"},
				"current_cn": "required"
			},
			"load": {
				"pool": "etl",
				"labels": {"role": "etl"},
				"fallback": "legacy-compatible",
				"empty_worker": "local-fallback"
			}
		}
	}`)
	require.NoError(t, err)

	tp := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:         WorkloadTP,
		ExecKind:      QueryExecTP,
		Tenant:        "tenant-a",
		IngressLabels: map[string]string{"role": "tp"},
		SchedulingIntent: SchedulingIntent{
			WorkerSet: WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, set)
	require.Equal(t, WorkloadRoutingLocal, tp.Routing)
	require.Equal(t, CurrentCNRequired, tp.Intent.CurrentCNPolicy)
	require.Equal(t, WorkerSetMax, tp.Intent.WorkerSet.Mode)
	require.Equal(t, 1, tp.Intent.WorkerSet.MaxWorkers)

	load := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:    WorkloadLoad,
		ExecKind: QueryExecTP,
		Tenant:   "tenant-a",
		SchedulingIntent: SchedulingIntent{
			WorkerSet: WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, set)
	require.Equal(t, WorkloadRoutingSingle, load.Routing)
	require.Equal(t, WorkerSetMax, load.Intent.WorkerSet.Mode)
	require.Equal(t, 1, load.Intent.WorkerSet.MaxWorkers)

	strengthened := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:    WorkloadLoad,
		ExecKind: QueryExecAPMultiCN,
		Tenant:   "tenant-a",
		SchedulingIntent: SchedulingIntent{
			PoolFallback:      PoolFallbackStrict,
			EmptyWorkerPolicy: EmptyWorkerFail,
			WorkerSet:         WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, set)
	require.Equal(t, PoolFallbackStrict, strengthened.Intent.PoolFallback)
	require.Equal(t, EmptyWorkerFail, strengthened.Intent.EmptyWorkerPolicy)

	legacy := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:         WorkloadAP,
		ExecKind:      QueryExecAPMultiCN,
		IngressLabels: map[string]string{"role": "tp"},
		LegacyPool:    "legacy",
		SchedulingIntent: SchedulingIntent{
			WorkerSet: WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, WorkloadPolicySet{})
	require.False(t, legacy.Applied)
	require.Equal(t, WorkloadPolicySourceLegacy, legacy.Source)
	require.Equal(t, map[string]string{"role": "tp"}, legacy.Pool.Labels)
	require.Equal(t, "legacy", legacy.Intent.RequestedPool)
}

func TestResolveWorkloadPolicyFailsClosedWithoutAuthenticatedTenant(t *testing.T) {
	set, err := ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"}
			}
		}
	}`)
	require.NoError(t, err)

	policy := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:    WorkloadAP,
		ExecKind: QueryExecAPMultiCN,
		SchedulingIntent: SchedulingIntent{
			WorkerSet: WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, set)
	require.Equal(t, WorkloadPolicySourceAccountGlobal, policy.Source)
	require.Equal(t, "workload-policy-missing-tenant", policy.Reason)
	require.True(t, policy.Intent.Explicit)
	require.False(t, policy.Intent.PoolFallback.Valid())
}

func TestWorkloadLocalRoutingHonorsExplicitEmptyWorkerFallback(t *testing.T) {
	current := Worker{
		ID:    "tp-local",
		Addr:  "tp-local:6001",
		Route: WorkerRouteLocal,
	}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecTP,
		CurrentCN:       current,
		CurrentCNPolicy: CurrentCNRequired,
		Intent: SchedulingIntent{
			Explicit:          true,
			PoolFallback:      PoolFallbackLegacyCompatible,
			EmptyWorkerPolicy: EmptyWorkerLocalFallback,
			CurrentCNPolicy:   CurrentCNRequired,
			WorkerSet:         WorkerSetPolicy{Mode: WorkerSetAll},
		},
		WorkloadPolicy: EffectiveWorkloadPolicy{
			Applied: true,
			Routing: WorkloadRoutingLocal,
		},
	})

	require.True(t, decision.Satisfied)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.Equal(t, Workers{current}, decision.Workers)
	require.Zero(t, decision.EligibleCount)
}

func TestResolveWorkloadPolicyCannotMaskInvalidUserIntent(t *testing.T) {
	set, err := ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"}
			}
		}
	}`)
	require.NoError(t, err)

	policy := ResolveWorkloadPolicy(WorkloadDescriptor{
		Class:    WorkloadAP,
		ExecKind: QueryExecAPMultiCN,
		Tenant:   "tenant-a",
		SchedulingIntent: SchedulingIntent{
			PoolFallback: PoolFallbackPolicy(255),
			WorkerSet:    WorkerSetPolicy{Mode: WorkerSetAll},
		},
	}, set)
	require.Equal(t, WorkloadPolicySourceAccountGlobal, policy.Source)
	require.Equal(t, "invalid-user-scheduling-intent", policy.Reason)
	require.False(t, policy.Applied)
	require.False(t, policy.Intent.PoolFallback.Valid())
}

func TestWorkloadPolicyTraceAndExplainAreSelfDescribing(t *testing.T) {
	recorder := new(TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, QueryDecision{
		ExecKind: QueryExecAPOneCN,
		CurrentCN: Worker{
			ID: "tp", Addr: "tp:6001", Mcpu: 4, Route: WorkerRouteLocal,
		},
		Workers: Workers{{
			ID: "ap", Addr: "ap:6001", Mcpu: 8, Route: WorkerRouteRemote,
		}},
		Reason:          ReasonExcludedCurrentCN,
		CurrentCNPolicy: CurrentCNExcluded,
		Satisfied:       true,
		EligibleCount:   1,
		WorkloadPolicy: EffectiveWorkloadPolicy{
			WorkloadClass: WorkloadAP,
			Source:        WorkloadPolicySourceAccountGlobal,
			Generation:    "0123456789abcdef",
			Reason:        "matched-account-workload-policy",
			Applied:       true,
			Routing:       WorkloadRoutingSingle,
		},
		Intent: SchedulingIntent{
			Explicit:          true,
			RequestedPool:     "tenant-ap",
			PoolFallback:      PoolFallbackStrict,
			EmptyWorkerPolicy: EmptyWorkerFail,
			CurrentCNPolicy:   CurrentCNExcluded,
			WorkerSet: WorkerSetPolicy{
				Mode:             WorkerSetMax,
				MaxWorkers:       1,
				SelectionKey:     "statement",
				AlgorithmVersion: WorkerSelectionAlgorithmV1,
			},
		},
		ResolvedPool: ResolvedPoolDecision{
			RequestedIdentity: "tenant-ap",
			Identity:          "tenant-ap",
			Resolution:        PoolResolutionTenantLabels,
		},
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceClusterInventory,
			PoolResolution:  PoolResolutionTenantLabels,
			DiscoveredCount: 2,
		},
		ResolvedCandidateCount: 1,
	})

	trace := recorder.Snapshot()
	require.Equal(t, SchedulingTraceVersion, trace.Version)
	require.True(t, trace.PersistStandalone())
	query := trace.Attempts[0].Query
	require.Equal(t, "ap", query.WorkloadClass)
	require.Equal(t, "account-global", query.WorkloadPolicySource)
	require.Equal(t, "0123456789abcdef", query.WorkloadPolicyGeneration)
	require.Equal(t, "single-worker", query.WorkloadRouting)
	require.Equal(t, "ap", query.Selected[0].ID)
	explain := strings.Join(ExplainLines(trace), "\n")
	require.Contains(t, explain, "Workload: class=ap")
	require.Contains(t, explain, "policy-source=account-global")
	require.Contains(t, explain, "generation=0123456789abcdef")
}
