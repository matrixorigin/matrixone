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

package cache

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const catalogInvalidationAttributionEnv = "MO_CATALOG_INVALIDATION_ATTRIBUTION"

// Shadow state is diagnostic only. A hard cap makes retained memory explicit;
// once reached, the shadow becomes conservative for all subsequent queries
// instead of retaining unbounded tenant/table churn.
const catalogShadowEntryLimit = 1 << 16

// CatalogInvalidationConsumer identifies the production consumer that asked
// whether a catalog dependency is stale. It is intentionally small and
// stable because it is serialized in the experiment report.
type CatalogInvalidationConsumer uint8

const (
	CatalogInvalidationConsumerUnknown CatalogInvalidationConsumer = iota
	CatalogInvalidationConsumerPreparedPlan
	CatalogInvalidationConsumerRCTableCache
)

func (c CatalogInvalidationConsumer) String() string {
	switch c {
	case CatalogInvalidationConsumerPreparedPlan:
		return "prepared_plan"
	case CatalogInvalidationConsumerRCTableCache:
		return "rc_table_cache"
	default:
		return "unknown"
	}
}

type catalogShadowDatabaseKey struct {
	accountID uint32
	name      string
}

type catalogShadowTableKey struct {
	accountID    uint32
	databaseID   uint64
	databaseName string
	name         string
}

type catalogShadowDatabaseState struct {
	id        uint64
	ts        timestamp.Timestamp
	deleted   bool
	ambiguous bool
}

type catalogShadowTableState struct {
	id        uint64
	version   uint32
	ts        timestamp.Timestamp
	deleted   bool
	ambiguous bool
}

type catalogInvalidationCounters struct {
	checks               uint64
	invalidations        uint64
	bucketInvalidations  uint64
	preciseInvalidations uint64
	bucketFalsePositive  uint64
	bucketFalseNegative  uint64
	preciseFalsePositive uint64
	preciseFalseNegative uint64
}

type catalogLatencyHistogram struct {
	count   uint64
	success uint64
	error   uint64
	miss    uint64
	bucket  [13]uint64
}

// CatalogInvalidationOutcome describes the terminal result of a consumer
// rebuild/reload attempt in the attribution report.
type CatalogInvalidationOutcome uint8

const (
	CatalogInvalidationSuccess CatalogInvalidationOutcome = iota
	CatalogInvalidationError
	CatalogInvalidationMiss
)

// The histogram is deliberately bounded. The last bucket is an overflow
// bucket, so a long-running attribution process cannot retain one sample per
// request.
var catalogLatencyBounds = [...]time.Duration{
	1 * time.Microsecond,
	5 * time.Microsecond,
	10 * time.Microsecond,
	25 * time.Microsecond,
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	100 * time.Millisecond,
}

func (h *catalogLatencyHistogram) observe(d time.Duration) {
	h.record(d, CatalogInvalidationSuccess)
}

func (h *catalogLatencyHistogram) record(d time.Duration, outcome CatalogInvalidationOutcome) {
	h.count++
	switch outcome {
	case CatalogInvalidationSuccess:
		h.success++
	case CatalogInvalidationError:
		h.error++
	case CatalogInvalidationMiss:
		h.miss++
	}
	for i, bound := range catalogLatencyBounds {
		if d <= bound {
			h.bucket[i]++
			return
		}
	}
	h.bucket[len(h.bucket)-1]++
}

func (h catalogLatencyHistogram) quantile(q float64) int64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(float64(h.count-1)*q) + 1
	var seen uint64
	for i, count := range h.bucket {
		seen += count
		if seen >= target {
			if i >= len(catalogLatencyBounds) {
				return int64(catalogLatencyBounds[len(catalogLatencyBounds)-1]) * 10
			}
			return int64(catalogLatencyBounds[i])
		}
	}
	return int64(catalogLatencyBounds[len(catalogLatencyBounds)-1]) * 10
}

type catalogInvalidationAttribution struct {
	mu sync.Mutex

	counters         map[CatalogInvalidationConsumer]catalogInvalidationCounters
	preparedRebuilds catalogLatencyHistogram
	rcCacheReloads   catalogLatencyHistogram
	events           map[string]uint64

	accountLatest  map[uint32]timestamp.Timestamp
	databases      map[catalogShadowDatabaseKey]catalogShadowDatabaseState
	tables         map[catalogShadowTableKey]catalogShadowTableState
	metadata       CatalogInvalidationReportMetadata
	shadowOverflow bool
}

func newCatalogInvalidationAttribution() *catalogInvalidationAttribution {
	return &catalogInvalidationAttribution{
		counters:      make(map[CatalogInvalidationConsumer]catalogInvalidationCounters),
		events:        make(map[string]uint64),
		accountLatest: make(map[uint32]timestamp.Timestamp),
		databases:     make(map[catalogShadowDatabaseKey]catalogShadowDatabaseState),
		tables:        make(map[catalogShadowTableKey]catalogShadowTableState),
	}
}

func catalogInvalidationAttributionEnabledFromEnv() bool {
	return os.Getenv(catalogInvalidationAttributionEnv) == "1"
}

func (a *catalogInvalidationAttribution) observeTable(
	accountID uint32,
	databaseID uint64,
	databaseName string,
	name string,
	id uint64,
	version uint32,
	ts timestamp.Timestamp,
	deleted bool,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	event := "table_upsert"
	if deleted {
		event = "table_delete"
	}
	a.events[event]++
	if _, ok := a.accountLatest[accountID]; !ok && len(a.accountLatest) >= catalogShadowEntryLimit {
		a.shadowOverflow = true
	} else if latest, ok := a.accountLatest[accountID]; !ok || ts.Greater(latest) {
		a.accountLatest[accountID] = ts
	}
	key := catalogShadowTableKey{
		accountID: accountID, databaseID: databaseID, databaseName: databaseName, name: name,
	}
	state, ok := a.tables[key]
	if !ok || ts.Greater(state.ts) {
		if !ok && len(a.tables) >= catalogShadowEntryLimit {
			a.shadowOverflow = true
			return
		}
		a.tables[key] = catalogShadowTableState{id: id, version: version, ts: ts, deleted: deleted}
		return
	}
	if !state.ts.Greater(ts) && (state.id != id || state.version != version || state.deleted != deleted) {
		state.ambiguous = true
		a.tables[key] = state
	}
}

func (a *catalogInvalidationAttribution) observeDatabase(
	accountID uint32,
	name string,
	id uint64,
	ts timestamp.Timestamp,
	deleted bool,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	event := "database_insert"
	if deleted {
		event = "database_delete"
	}
	a.events[event]++
	key := catalogShadowDatabaseKey{accountID: accountID, name: name}
	state, ok := a.databases[key]
	if !ok || ts.Greater(state.ts) {
		if !ok && len(a.databases) >= catalogShadowEntryLimit {
			a.shadowOverflow = true
			return
		}
		a.databases[key] = catalogShadowDatabaseState{id: id, ts: ts, deleted: deleted}
		return
	}
	if !state.ts.Greater(ts) && (state.id != id || state.deleted != deleted) {
		state.ambiguous = true
		a.databases[key] = state
	}
}

func (a *catalogInvalidationAttribution) preciseDecision(qry *TableChangeQuery) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.shadowOverflow {
		return true
	}
	if qry.DatabaseName != "" {
		state, ok := a.databases[catalogShadowDatabaseKey{accountID: qry.AccountId, name: qry.DatabaseName}]
		if ok && state.ts.Greater(qry.Ts) && (state.deleted || state.id != qry.DatabaseId || state.ambiguous) {
			return true
		}
	}
	if qry.Name == "" {
		if qry.DatabaseId == 0 {
			return a.accountLatest[qry.AccountId].Greater(qry.Ts)
		}
		return false
	}
	state, ok := a.tables[catalogShadowTableKey{
		accountID: qry.AccountId, databaseID: qry.DatabaseId,
		databaseName: qry.DatabaseName, name: qry.Name,
	}]
	if !ok || !state.ts.Greater(qry.Ts) {
		return false
	}
	return state.deleted || state.id != qry.TableId || state.version > qry.Version || state.ambiguous
}

func (a *catalogInvalidationAttribution) recordDecision(
	consumer CatalogInvalidationConsumer,
	exact bool,
	bucket bool,
	precise bool,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.counters[consumer]
	c.checks++
	if exact {
		c.invalidations++
	}
	if bucket {
		c.bucketInvalidations++
	}
	if precise {
		c.preciseInvalidations++
	}
	if bucket && !exact {
		c.bucketFalsePositive++
	}
	if exact && !bucket {
		c.bucketFalseNegative++
	}
	if precise && !exact {
		c.preciseFalsePositive++
	}
	if exact && !precise {
		c.preciseFalseNegative++
	}
	a.counters[consumer] = c
}

func (a *catalogInvalidationAttribution) recordPreparedRebuild(d time.Duration, success bool) {
	a.mu.Lock()
	outcome := CatalogInvalidationError
	if success {
		outcome = CatalogInvalidationSuccess
	}
	a.preparedRebuilds.record(d, outcome)
	a.mu.Unlock()
}

func (a *catalogInvalidationAttribution) recordRCTableCacheReload(d time.Duration, outcome CatalogInvalidationOutcome) {
	a.mu.Lock()
	a.rcCacheReloads.record(d, outcome)
	a.mu.Unlock()
}

// CatalogInvalidationCounter is the JSON-safe per-consumer counter view.
type CatalogInvalidationCounter struct {
	Checks                uint64 `json:"checks"`
	Invalidations         uint64 `json:"invalidations"`
	BucketInvalidations   uint64 `json:"bucket_invalidations"`
	PreciseInvalidations  uint64 `json:"precise_invalidations"`
	BucketFalsePositives  uint64 `json:"bucket_false_positives"`
	BucketFalseNegatives  uint64 `json:"bucket_false_negatives"`
	PreciseFalsePositives uint64 `json:"precise_false_positives"`
	PreciseFalseNegatives uint64 `json:"precise_false_negatives"`
}

// CatalogInvalidationReportMetadata identifies the experiment input and
// collection window. The cache does not infer these values from the process;
// the harness records them explicitly before publishing an artifact.
type CatalogInvalidationReportMetadata struct {
	MatrixONESHA string `json:"matrixone_sha,omitempty"`
	Config       string `json:"config,omitempty"`
	Window       string `json:"window,omitempty"`
	Integrity    string `json:"integrity,omitempty"`
}

// CatalogInvalidationLatency is the bounded latency view used in the report.
type CatalogInvalidationLatency struct {
	Count   uint64 `json:"count"`
	Success uint64 `json:"successes"`
	Error   uint64 `json:"errors"`
	Miss    uint64 `json:"misses"`
	P50NS   int64  `json:"p50_ns"`
	P95NS   int64  `json:"p95_ns"`
	P99NS   int64  `json:"p99_ns"`
}

// CatalogInvalidationShadow is a bounded-state summary of the precise shadow
// oracle. The retained-byte value is an estimate for experiment comparison,
// not an allocation-accounting claim.
type CatalogInvalidationShadow struct {
	Accounts               int    `json:"accounts"`
	DatabaseEntries        int    `json:"database_entries"`
	TableEntries           int    `json:"table_entries"`
	Overflow               bool   `json:"overflow"`
	EstimatedRetainedBytes uint64 `json:"estimated_retained_bytes"`
}

// CatalogInvalidationReport is stable JSON output for the attribution PR.
type CatalogInvalidationReport struct {
	SchemaVersion       int                                   `json:"schema_version"`
	Enabled             bool                                  `json:"enabled"`
	Metadata            CatalogInvalidationReportMetadata     `json:"metadata"`
	Consumers           map[string]CatalogInvalidationCounter `json:"consumers"`
	Events              map[string]uint64                     `json:"events"`
	PreparedPlanRebuild CatalogInvalidationLatency            `json:"prepared_plan_rebuild"`
	RCTableCacheReload  CatalogInvalidationLatency            `json:"rc_table_cache_reload"`
	Shadow              CatalogInvalidationShadow             `json:"shadow"`
}

func histogramReport(h catalogLatencyHistogram) CatalogInvalidationLatency {
	return CatalogInvalidationLatency{
		Count: h.count, Success: h.success, Error: h.error, Miss: h.miss,
		P50NS: h.quantile(0.50), P95NS: h.quantile(0.95), P99NS: h.quantile(0.99),
	}
}

func (a *catalogInvalidationAttribution) report() CatalogInvalidationReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	consumers := make(map[string]CatalogInvalidationCounter, len(a.counters))
	for consumer, c := range a.counters {
		consumers[consumer.String()] = CatalogInvalidationCounter{
			Checks:                c.checks,
			Invalidations:         c.invalidations,
			BucketInvalidations:   c.bucketInvalidations,
			PreciseInvalidations:  c.preciseInvalidations,
			BucketFalsePositives:  c.bucketFalsePositive,
			BucketFalseNegatives:  c.bucketFalseNegative,
			PreciseFalsePositives: c.preciseFalsePositive,
			PreciseFalseNegatives: c.preciseFalseNegative,
		}
	}
	accounts := make(map[uint32]struct{}, len(a.accountLatest))
	for account := range a.accountLatest {
		accounts[account] = struct{}{}
	}
	var retained uint64
	for key := range a.databases {
		retained += uint64(stringSize(key.name) + 32)
	}
	for key := range a.tables {
		retained += uint64(stringSize(key.name) + len(key.databaseName) + 40)
	}
	return CatalogInvalidationReport{
		SchemaVersion:       1,
		Enabled:             true,
		Metadata:            a.metadata,
		Consumers:           consumers,
		Events:              cloneUint64Map(a.events),
		PreparedPlanRebuild: histogramReport(a.preparedRebuilds),
		RCTableCacheReload:  histogramReport(a.rcCacheReloads),
		Shadow: CatalogInvalidationShadow{
			Accounts: len(accounts), DatabaseEntries: len(a.databases),
			TableEntries: len(a.tables), Overflow: a.shadowOverflow,
			EstimatedRetainedBytes: retained,
		},
	}
}

func cloneUint64Map(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func stringSize(value string) int {
	return len(value)
}

// SetCatalogInvalidationReportMetadata attaches exact identity information to
// the next report. It is intended for an experiment harness before collection
// starts and is not a production configuration API.
func (cc *CatalogCache) SetCatalogInvalidationReportMetadata(metadata CatalogInvalidationReportMetadata) {
	if cc.attribution == nil {
		return
	}
	cc.attribution.mu.Lock()
	cc.attribution.metadata = metadata
	cc.attribution.mu.Unlock()
}

// CatalogInvalidationAttributionEnabled reports whether this cache has the
// opt-in experiment state. The pointer is initialized once during startup.
func (cc *CatalogCache) CatalogInvalidationAttributionEnabled() bool {
	return cc.attribution != nil
}

func (cc *CatalogCache) RecordPreparedPlanRebuild(d time.Duration, success bool) {
	if cc.attribution != nil {
		cc.attribution.recordPreparedRebuild(d, success)
	}
}

func (cc *CatalogCache) RecordRCTableCacheReload(d time.Duration, outcome CatalogInvalidationOutcome) {
	if cc.attribution != nil {
		cc.attribution.recordRCTableCacheReload(d, outcome)
	}
}

// SnapshotCatalogInvalidationReport returns a bounded, JSON-safe snapshot.
func (cc *CatalogCache) SnapshotCatalogInvalidationReport() CatalogInvalidationReport {
	if cc.attribution == nil {
		return CatalogInvalidationReport{
			SchemaVersion: 1,
			Enabled:       false,
			Consumers:     map[string]CatalogInvalidationCounter{},
			Events:        map[string]uint64{},
		}
	}
	return cc.attribution.report()
}

// WriteCatalogInvalidationReport writes the report without prescribing a
// filesystem or service endpoint. Experiment harnesses choose the artifact
// path and can include the exact binary/config identity alongside it.
func (cc *CatalogCache) WriteCatalogInvalidationReport(w io.Writer) error {
	return json.NewEncoder(w).Encode(cc.SnapshotCatalogInvalidationReport())
}
