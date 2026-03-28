// Copyright 2024 Matrix Origin
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

package disttae

import (
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type DebugCatalogState struct {
	LatestLogtailAppliedTS timestamp.Timestamp       `json:"latest_logtail_applied_ts"`
	CatalogCache           cache.DebugCatalogSummary `json:"catalog_cache"`
	LazyCatalog            DebugLazyCatalogState     `json:"lazy_catalog"`
	Account                *DebugCatalogAccountState `json:"account,omitempty"`
}

type DebugLazyCatalogState struct {
	Enabled                        bool                    `json:"enabled"`
	CatchingUpCount                int                     `json:"catching_up_count"`
	PendingActivationResponseCount int                     `json:"pending_activation_response_count"`
	InflightActivationCount        int                     `json:"inflight_activation_count"`
	WantedAccounts                 []uint32                `json:"wanted_accounts"`
	Accounts                       []DebugLazyAccountState `json:"accounts"`
}

type DebugLazyAccountState struct {
	AccountID         uint32               `json:"account_id"`
	State             string               `json:"state"`
	ReadyTS           *timestamp.Timestamp `json:"ready_ts,omitempty"`
	PendingSeq        uint64               `json:"pending_seq"`
	DelayedApplyCount int                  `json:"delayed_apply_count"`
}

type DebugCatalogAccountState struct {
	AccountID              uint32               `json:"account_id"`
	Present                bool                 `json:"present"`
	State                  string               `json:"state"`
	InWantedAccounts       bool                 `json:"in_wanted_accounts"`
	PendingSeq             uint64               `json:"pending_seq"`
	DelayedApplyCount      int                  `json:"delayed_apply_count"`
	ReadyTS                *timestamp.Timestamp `json:"ready_ts,omitempty"`
	LatestLogtailAppliedTS timestamp.Timestamp  `json:"latest_logtail_applied_ts"`
	RequestedSnapshotTS    *timestamp.Timestamp `json:"requested_snapshot_ts,omitempty"`
	CanServeLatest         bool                 `json:"can_serve_latest"`
	CanServeSnapshot       *bool                `json:"can_serve_snapshot,omitempty"`
}

type DebugCatalogCacheState struct {
	AccountID              uint32                            `json:"account_id"`
	LatestLogtailAppliedTS timestamp.Timestamp               `json:"latest_logtail_applied_ts"`
	SnapshotTS             timestamp.Timestamp               `json:"snapshot_ts"`
	DatabaseFilter         string                            `json:"database_filter,omitempty"`
	CanServeLatest         bool                              `json:"can_serve_latest"`
	CanServeSnapshot       bool                              `json:"can_serve_snapshot"`
	Contents               cache.DebugCatalogAccountContents `json:"contents"`
}

type DebugCatalogActivationState struct {
	LatestLogtailAppliedTS timestamp.Timestamp           `json:"latest_logtail_applied_ts"`
	ReturnedEventCount     int                           `json:"returned_event_count"`
	Events                 []DebugCatalogActivationEvent `json:"events"`
}

type DebugCatalogActivationEvent struct {
	AccountID         uint32               `json:"account_id"`
	Seq               uint64               `json:"seq"`
	Source            string               `json:"source"`
	Phase             string               `json:"phase"`
	Result            string               `json:"result"`
	Error             string               `json:"error,omitempty"`
	TargetTS          *timestamp.Timestamp `json:"target_ts,omitempty"`
	ReplayTS          *timestamp.Timestamp `json:"replay_ts,omitempty"`
	DelayedApplyCount int                  `json:"delayed_apply_count,omitempty"`
	StartedAt         *time.Time           `json:"started_at,omitempty"`
	FinishedAt        *time.Time           `json:"finished_at,omitempty"`
}

type DebugPartitionsState struct {
	LatestLogtailAppliedTS timestamp.Timestamp   `json:"latest_logtail_applied_ts"`
	LivePartitionCount     int                   `json:"live_partition_count"`
	ReturnedPartitionCount int                   `json:"returned_partition_count"`
	SnapshotConfig         DebugSnapshotGCConfig `json:"snapshot_config"`
	SnapshotMetrics        DebugSnapshotMetrics  `json:"snapshot_metrics"`
	Partitions             []DebugPartitionInfo  `json:"partitions"`
}

type DebugSnapshotGCConfig struct {
	Enabled              bool   `json:"enabled"`
	GCInterval           string `json:"gc_interval"`
	MaxAge               string `json:"max_age"`
	MaxSnapshotsPerTable int    `json:"max_snapshots_per_table"`
	MaxTotalSnapshots    int    `json:"max_total_snapshots"`
}

type DebugSnapshotMetrics struct {
	TotalSnapshots  int64 `json:"total_snapshots"`
	TotalTables     int64 `json:"total_tables"`
	GCRuns          int64 `json:"gc_runs"`
	SnapshotsGCed   int64 `json:"snapshots_gced"`
	LastGCDuration  int64 `json:"last_gc_duration_ns"`
	SnapshotHits    int64 `json:"snapshot_hits"`
	SnapshotMisses  int64 `json:"snapshot_misses"`
	SnapshotCreates int64 `json:"snapshot_creates"`
	LRUEvictions    int64 `json:"lru_evictions"`
	AgeEvictions    int64 `json:"age_evictions"`
}

type DebugPartitionInfo struct {
	DatabaseID         uint64                                   `json:"database_id"`
	DatabaseName       string                                   `json:"database_name,omitempty"`
	TableID            uint64                                   `json:"table_id"`
	TableName          string                                   `json:"table_name,omitempty"`
	TableInfoKnown     bool                                     `json:"table_info_known"`
	StateMaterialized  bool                                     `json:"state_materialized"`
	CheckpointConsumed bool                                     `json:"checkpoint_consumed"`
	State              logtailreplay.DebugPartitionStateSummary `json:"state"`
}

type debugPartitionRef struct {
	dbID  uint64
	tblID uint64
	part  *logtailreplay.Partition
}

const debugActivationHistoryLimit = 128

func (e *Engine) DebugCatalogState(accountFilter *uint32, snapshotFilter *timestamp.Timestamp) DebugCatalogState {
	var state DebugCatalogState
	if e == nil {
		return state
	}

	state.LatestLogtailAppliedTS = e.PushClient().LatestLogtailAppliedTime()
	if cc := e.GetLatestCatalogCache(); cc != nil {
		state.CatalogCache = cc.DebugSummary()
	}
	state.LazyCatalog = e.PushClient().DebugLazyCatalogState(accountFilter)
	if accountFilter != nil {
		accountState := e.PushClient().DebugCatalogAccountState(*accountFilter)
		accountState.LatestLogtailAppliedTS = state.LatestLogtailAppliedTS
		accountState.CanServeLatest = debugCanServeAccountAt(e, *accountFilter, state.LatestLogtailAppliedTS)
		if snapshotFilter != nil {
			ts := *snapshotFilter
			accountState.RequestedSnapshotTS = &ts
			canServe := debugCanServeAccountAt(e, *accountFilter, ts)
			accountState.CanServeSnapshot = &canServe
		}
		state.Account = &accountState
	}
	return state
}

func (c *PushClient) DebugLazyCatalogState(accountFilter *uint32) DebugLazyCatalogState {
	if c == nil || c.lazyCatalog == nil {
		return DebugLazyCatalogState{}
	}
	return c.lazyCatalog.debugState(accountFilter)
}

func (c *PushClient) DebugCatalogAccountState(accountID uint32) DebugCatalogAccountState {
	state := DebugCatalogAccountState{
		AccountID: accountID,
		State:     "ungated",
	}
	if c == nil || c.lazyCatalog == nil {
		return state
	}
	return c.lazyCatalog.debugAccountState(accountID)
}

func (c *PushClient) DebugCatalogActivationHistory(accountFilter *uint32, limit int) DebugCatalogActivationState {
	state := DebugCatalogActivationState{
		Events: make([]DebugCatalogActivationEvent, 0),
	}
	if c == nil {
		return state
	}
	state.LatestLogtailAppliedTS = c.LatestLogtailAppliedTime()
	if c.lazyCatalog == nil {
		return state
	}
	state.Events = c.lazyCatalog.debugActivationHistory(accountFilter, limit)
	state.ReturnedEventCount = len(state.Events)
	return state
}

func (s *lazyCatalogCNState) debugState(accountFilter *uint32) DebugLazyCatalogState {
	if s == nil {
		return DebugLazyCatalogState{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := DebugLazyCatalogState{
		Enabled:                        s.enabled.Load(),
		CatchingUpCount:                int(s.catchingUpCount.Load()),
		PendingActivationResponseCount: len(s.pendingActivationResponses),
		WantedAccounts:                 make([]uint32, 0),
		Accounts:                       make([]DebugLazyAccountState, 0),
	}

	s.inflightActivations.Range(func(_, _ any) bool {
		state.InflightActivationCount++
		return true
	})

	for accountID := range s.wantedAccounts {
		if accountFilter != nil && *accountFilter != accountID {
			continue
		}
		state.WantedAccounts = append(state.WantedAccounts, accountID)
	}
	sort.Slice(state.WantedAccounts, func(i, j int) bool {
		return state.WantedAccounts[i] < state.WantedAccounts[j]
	})

	for accountID, entry := range s.accounts {
		if accountFilter != nil && *accountFilter != accountID {
			continue
		}
		accountState := DebugLazyAccountState{
			AccountID:         accountID,
			State:             accountReadyStateString(entry.state),
			PendingSeq:        s.pendingSeq[accountID],
			DelayedApplyCount: len(s.accountDCA[accountID]),
		}
		if entry.state == accountReady && (entry.readyTS.PhysicalTime != 0 || entry.readyTS.LogicalTime != 0) {
			ts := entry.readyTS
			accountState.ReadyTS = &ts
		}
		state.Accounts = append(state.Accounts, accountState)
	}
	sort.Slice(state.Accounts, func(i, j int) bool {
		return state.Accounts[i].AccountID < state.Accounts[j].AccountID
	})

	return state
}

func (s *lazyCatalogCNState) debugAccountState(accountID uint32) DebugCatalogAccountState {
	state := DebugCatalogAccountState{
		AccountID: accountID,
		State:     "ungated",
	}
	if s == nil {
		return state
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.enabled.Load() {
		return state
	}
	state.State = "absent"
	_, state.InWantedAccounts = s.wantedAccounts[accountID]

	entry, ok := s.accounts[accountID]
	if !ok {
		return state
	}
	state.Present = true
	state.State = accountReadyStateString(entry.state)
	state.PendingSeq = s.pendingSeq[accountID]
	state.DelayedApplyCount = len(s.accountDCA[accountID])
	if entry.state == accountReady && (entry.readyTS.PhysicalTime != 0 || entry.readyTS.LogicalTime != 0) {
		ts := entry.readyTS
		state.ReadyTS = &ts
	}
	return state
}

func (s *lazyCatalogCNState) recordActivationEvent(event DebugCatalogActivationEvent) {
	if s == nil {
		return
	}
	cloned := cloneDebugCatalogActivationEvent(event)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.activationHistory) >= debugActivationHistoryLimit {
		copy(s.activationHistory, s.activationHistory[1:])
		s.activationHistory[len(s.activationHistory)-1] = cloned
		return
	}
	s.activationHistory = append(s.activationHistory, cloned)
}

func (s *lazyCatalogCNState) debugActivationHistory(accountFilter *uint32, limit int) []DebugCatalogActivationEvent {
	if s == nil {
		return []DebugCatalogActivationEvent{}
	}
	if limit <= 0 {
		limit = 50
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events := make([]DebugCatalogActivationEvent, 0, min(limit, len(s.activationHistory)))
	for i := len(s.activationHistory) - 1; i >= 0 && len(events) < limit; i-- {
		event := s.activationHistory[i]
		if accountFilter != nil && event.AccountID != *accountFilter {
			continue
		}
		events = append(events, cloneDebugCatalogActivationEvent(event))
	}
	return events
}

func accountReadyStateString(state catalogReadyState) string {
	switch state {
	case accountInactive:
		return "inactive"
	case accountCatchingUp:
		return "catching_up"
	case accountReadyDraining:
		return "ready_draining"
	case accountReady:
		return "ready"
	default:
		return "unknown"
	}
}

func (e *Engine) DebugCatalogCache(
	accountID uint32,
	snapshotFilter *timestamp.Timestamp,
	dbFilter string,
	limit int,
) DebugCatalogCacheState {
	state := DebugCatalogCacheState{
		AccountID: accountID,
	}
	if e == nil {
		return state
	}

	state.LatestLogtailAppliedTS = e.PushClient().LatestLogtailAppliedTime()
	state.SnapshotTS = state.LatestLogtailAppliedTS
	if snapshotFilter != nil {
		state.SnapshotTS = *snapshotFilter
	}
	state.DatabaseFilter = dbFilter
	state.CanServeLatest = debugCanServeAccountAt(e, accountID, state.LatestLogtailAppliedTS)
	state.CanServeSnapshot = debugCanServeAccountAt(e, accountID, state.SnapshotTS)

	if cc := e.GetLatestCatalogCache(); cc != nil {
		state.Contents = cc.DebugAccountContents(accountID, state.SnapshotTS, dbFilter, limit)
	}
	return state
}

func (e *Engine) DebugCatalogActivationHistory(
	accountFilter *uint32,
	limit int,
) DebugCatalogActivationState {
	if e == nil {
		return DebugCatalogActivationState{}
	}
	return e.PushClient().DebugCatalogActivationHistory(accountFilter, limit)
}

func (e *Engine) DebugPartitions(dbFilter, tableFilter *uint64, limit int) DebugPartitionsState {
	state := DebugPartitionsState{}
	if e == nil {
		return state
	}

	state.LatestLogtailAppliedTS = e.PushClient().LatestLogtailAppliedTime()
	if e.snapshotMgr != nil {
		cfg := e.snapshotMgr.GetConfig()
		metrics := e.snapshotMgr.GetMetrics()
		state.SnapshotConfig = DebugSnapshotGCConfig{
			Enabled:              cfg.Enabled,
			GCInterval:           cfg.GCInterval.String(),
			MaxAge:               cfg.MaxAge.String(),
			MaxSnapshotsPerTable: cfg.MaxSnapshotsPerTable,
			MaxTotalSnapshots:    cfg.MaxTotalSnapshots,
		}
		state.SnapshotMetrics = DebugSnapshotMetrics{
			TotalSnapshots:  metrics.TotalSnapshots.Load(),
			TotalTables:     metrics.TotalTables.Load(),
			GCRuns:          metrics.GCRuns.Load(),
			SnapshotsGCed:   metrics.SnapshotsGCed.Load(),
			LastGCDuration:  metrics.LastGCDuration.Load(),
			SnapshotHits:    metrics.SnapshotHits.Load(),
			SnapshotMisses:  metrics.SnapshotMisses.Load(),
			SnapshotCreates: metrics.SnapshotCreates.Load(),
			LRUEvictions:    metrics.LRUEvictions.Load(),
			AgeEvictions:    metrics.AgeEvictions.Load(),
		}
	}

	if limit <= 0 {
		limit = 100
	}

	refs := make([]debugPartitionRef, 0)
	e.RLock()
	for key, part := range e.partitions {
		if dbFilter != nil && key[0] != *dbFilter {
			continue
		}
		if tableFilter != nil && key[1] != *tableFilter {
			continue
		}
		refs = append(refs, debugPartitionRef{
			dbID:  key[0],
			tblID: key[1],
			part:  part,
		})
	}
	e.RUnlock()

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].dbID == refs[j].dbID {
			return refs[i].tblID < refs[j].tblID
		}
		return refs[i].dbID < refs[j].dbID
	})

	state.LivePartitionCount = len(refs)
	if len(refs) > limit {
		refs = refs[:limit]
	}
	state.ReturnedPartitionCount = len(refs)

	state.Partitions = make([]DebugPartitionInfo, 0, len(refs))
	for _, ref := range refs {
		info := DebugPartitionInfo{
			DatabaseID:         ref.dbID,
			DatabaseName:       debugPartitionDatabaseName(ref.dbID),
			TableID:            ref.tblID,
			CheckpointConsumed: ref.part.CheckpointConsumed(),
		}
		if ref.part.TableInfoOK && ref.part.TableInfo.Name != "" {
			info.TableName = ref.part.TableInfo.Name
			info.TableInfoKnown = true
		} else if name := debugPartitionFallbackTableName(ref.tblID); name != "" {
			info.TableName = name
			info.TableInfoKnown = true
		}
		if ps := ref.part.Snapshot(); ps != nil {
			info.StateMaterialized = true
			info.State = ps.DebugSummary()
		}
		state.Partitions = append(state.Partitions, info)
	}

	return state
}

func debugCanServeAccountAt(e *Engine, accountID uint32, ts timestamp.Timestamp) bool {
	if e == nil || ts.IsEmpty() {
		return false
	}
	cc := e.GetLatestCatalogCache()
	if cc == nil || !cc.CanServe(types.TimestampToTS(ts)) {
		return false
	}
	return e.PushClient().CanServeAccount(accountID, ts)
}

func debugPartitionDatabaseName(databaseID uint64) string {
	if databaseID == catalog.MO_CATALOG_ID {
		return catalog.MO_CATALOG
	}
	return ""
}

func debugPartitionFallbackTableName(tableID uint64) string {
	switch tableID {
	case catalog.MO_DATABASE_ID:
		return catalog.MO_DATABASE
	case catalog.MO_TABLES_ID:
		return catalog.MO_TABLES
	case catalog.MO_COLUMNS_ID:
		return catalog.MO_COLUMNS
	default:
		return ""
	}
}

func cloneDebugCatalogActivationEvent(event DebugCatalogActivationEvent) DebugCatalogActivationEvent {
	return DebugCatalogActivationEvent{
		AccountID:         event.AccountID,
		Seq:               event.Seq,
		Source:            event.Source,
		Phase:             event.Phase,
		Result:            event.Result,
		Error:             event.Error,
		TargetTS:          clonePtr(event.TargetTS),
		ReplayTS:          clonePtr(event.ReplayTS),
		DelayedApplyCount: event.DelayedApplyCount,
		StartedAt:         clonePtr(event.StartedAt),
		FinishedAt:        clonePtr(event.FinishedAt),
	}
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
