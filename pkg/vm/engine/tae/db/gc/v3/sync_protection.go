// Copyright 2021 Matrix Origin
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

package gc

import (
	"bytes"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"go.uber.org/zap"
)

const (
	// DefaultSyncProtectionTTL is the default TTL for sync protection
	// If a protection is not renewed within this duration, it will be force cleaned
	DefaultSyncProtectionTTL = 20 * time.Minute

	// DefaultMaxSyncProtections is the default maximum number of sync protections
	// Set to a large value to support many concurrent sync jobs
	// Jobs may take ~1.5 hours to be cleaned up after completion
	DefaultMaxSyncProtections = 1000000
)

// SyncProtection represents a single sync protection entry
type SyncProtection struct {
	JobID            string            // Sync job ID
	BF               index.BloomFilter // BloomFilter for protected objects (using xorfilter, deterministic)
	ValidTS          int64             // Renewal timestamp or absolute expiry, in nanoseconds
	ExpiresAtValidTS bool              // ValidTS is an absolute terminal expiry rather than a renewal timestamp
	SoftDelete       bool              // Whether soft deleted
	CreateTime       time.Time         // Creation time for logging
}

// EnsureSyncProtection makes crash replay idempotent while still rejecting a
// read reference that is already bound to different protection facts.
func (m *SyncProtectionManager) EnsureSyncProtection(jobID, bfData string, validTS int64, taskID string) error {
	guard, err := m.BeginProtection()
	if err != nil {
		return err
	}
	defer guard.Close()
	return guard.EnsureSyncProtection(jobID, bfData, validTS, taskID)
}

func (m *SyncProtectionManager) ensureSyncProtection(jobID, bfData string, validTS int64, taskID string, expiresAtValidTS bool) (*SyncProtection, bool, error) {
	registered, err := m.registerSyncProtection(jobID, bfData, validTS, taskID, expiresAtValidTS)
	if err == nil {
		return registered, true, nil
	}
	if !moerr.IsMoErrCode(err, moerr.ErrSyncProtectionExists) {
		return nil, false, err
	}
	expected, decodeErr := base64.StdEncoding.DecodeString(bfData)
	if decodeErr != nil {
		return nil, false, moerr.NewSyncProtectionInvalidNoCtx()
	}
	m.RLock()
	p := m.protections[jobID]
	if p == nil || p.SoftDelete || p.ValidTS != validTS || p.ExpiresAtValidTS != expiresAtValidTS {
		m.RUnlock()
		return nil, false, err
	}
	actual, marshalErr := p.BF.Marshal()
	m.RUnlock()
	if marshalErr != nil || !bytes.Equal(actual, expected) {
		return nil, false, err
	}
	return p, false, nil
}

// SyncProtectionManager manages sync protection entries
type SyncProtectionManager struct {
	sync.RWMutex
	protections       map[string]*SyncProtection // jobID -> protection
	protectionBarrier sync.RWMutex               // excludes GC from snapshot-to-registration handoffs
	gcRunning         atomic.Bool                // Whether GC is running
	ttl               time.Duration              // TTL for non-soft-deleted protections
	maxCount          int                        // Maximum number of protections
}

// SyncProtectionGuard prevents GC from starting while a caller enumerates a
// snapshot and installs its matching protection.
type SyncProtectionGuard struct {
	manager *SyncProtectionManager
	mu      sync.Mutex
	once    sync.Once
	owned   map[string]*SyncProtection
	closed  bool
}

// BeginProtection acquires the read side of the GC handoff barrier without
// waiting. Once GC has started (or is waiting to start), new work fails fast.
func (m *SyncProtectionManager) BeginProtection() (*SyncProtectionGuard, error) {
	if m == nil || !m.protectionBarrier.TryRLock() {
		return nil, moerr.NewGCIsRunningNoCtx()
	}
	if m.gcRunning.Load() {
		m.protectionBarrier.RUnlock()
		return nil, moerr.NewGCIsRunningNoCtx()
	}
	return &SyncProtectionGuard{manager: m}, nil
}

func (g *SyncProtectionGuard) EnsureSyncProtection(jobID, bfData string, validTS int64, taskID string) error {
	return g.ensureSyncProtection(jobID, bfData, validTS, taskID, false)
}

// EnsureExpiringSyncProtection registers a protection whose ValidTS is its
// absolute terminal expiry. It is used by fixed-lifetime read capabilities,
// which must not inherit the renewal TTL after their authority has expired.
func (g *SyncProtectionGuard) EnsureExpiringSyncProtection(jobID, bfData string, validTS int64, taskID string) error {
	return g.ensureSyncProtection(jobID, bfData, validTS, taskID, true)
}

func (g *SyncProtectionGuard) ensureSyncProtection(jobID, bfData string, validTS int64, taskID string, expiresAtValidTS bool) error {
	if g == nil || g.manager == nil {
		return moerr.NewSyncProtectionInvalidNoCtx()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return moerr.NewSyncProtectionInvalidNoCtx()
	}
	protection, created, err := g.manager.ensureSyncProtection(jobID, bfData, validTS, taskID, expiresAtValidTS)
	if err == nil && created {
		if g.owned == nil {
			g.owned = make(map[string]*SyncProtection)
		}
		g.owned[jobID] = protection
	}
	return err
}

// RollbackSyncProtection removes the exact registration created by this guard.
// An idempotently accepted preexisting registration belongs to another
// generation and is never removed. The guard's GC barrier makes immediate
// removal safe: GC cannot have observed a new registration while the
// admission/replay handoff is incomplete.
func (g *SyncProtectionGuard) RollbackSyncProtection(jobID string) error {
	if g == nil || g.manager == nil {
		return moerr.NewSyncProtectionInvalidNoCtx()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return moerr.NewSyncProtectionInvalidNoCtx()
	}
	owned := g.owned[jobID]
	if owned == nil {
		return nil
	}
	g.manager.Lock()
	if g.manager.protections[jobID] == owned {
		delete(g.manager.protections, jobID)
	}
	g.manager.Unlock()
	delete(g.owned, jobID)
	return nil
}

func (g *SyncProtectionGuard) Close() {
	if g == nil || g.manager == nil {
		return
	}
	g.once.Do(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.closed = true
		g.owned = nil
		g.manager.protectionBarrier.RUnlock()
	})
}

// NewSyncProtectionManager creates a new SyncProtectionManager
func NewSyncProtectionManager() *SyncProtectionManager {
	return &SyncProtectionManager{
		protections: make(map[string]*SyncProtection),
		ttl:         DefaultSyncProtectionTTL,
		maxCount:    DefaultMaxSyncProtections,
	}
}

// SetGCRunning sets the GC running state
func (m *SyncProtectionManager) SetGCRunning(running bool) {
	if running {
		m.protectionBarrier.Lock()
		m.gcRunning.Store(true)
	} else {
		m.gcRunning.Store(false)
		m.protectionBarrier.Unlock()
	}
	logutil.Debug(
		"GC-Sync-Protection-GC-State-Changed",
		zap.Bool("running", running),
	)
}

// IsGCRunning returns whether GC is running
func (m *SyncProtectionManager) IsGCRunning() bool {
	return m.gcRunning.Load()
}

// RegisterSyncProtection registers a new sync protection with BloomFilter
// bfData is base64 encoded BloomFilter bytes (using index.BloomFilter/xorfilter format)
// taskID is the CCPR iteration task ID with LSN (e.g., "taskID-123") for logging
// Returns error if GC is running or job already exists
func (m *SyncProtectionManager) RegisterSyncProtection(
	jobID string,
	bfData string,
	validTS int64,
	taskID string,
) error {
	guard, err := m.BeginProtection()
	if err != nil {
		return err
	}
	defer guard.Close()
	_, err = m.registerSyncProtection(jobID, bfData, validTS, taskID, false)
	return err
}

func (m *SyncProtectionManager) registerSyncProtection(
	jobID string,
	bfData string,
	validTS int64,
	taskID string,
	expiresAtValidTS bool,
) (*SyncProtection, error) {
	m.Lock()
	defer m.Unlock()

	// Check if GC is running
	if m.gcRunning.Load() {
		logutil.Warn(
			"GC-Sync-Protection-Register-Rejected-GC-Running",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
		)
		return nil, moerr.NewGCIsRunningNoCtx()
	}

	// Check if job already exists
	if _, ok := m.protections[jobID]; ok {
		logutil.Warn(
			"GC-Sync-Protection-Register-Already-Exists",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
		)
		return nil, moerr.NewSyncProtectionExistsNoCtx(jobID)
	}

	// Check max count
	if len(m.protections) >= m.maxCount {
		// Reclaim terminal entries only on the rejection slow path. This keeps
		// normal registration O(1) while preventing expired fixed-lifetime
		// capabilities from consuming all protection capacity between GC runs.
		m.cleanupExpiredAtLocked(time.Now())
	}
	if len(m.protections) >= m.maxCount {
		logutil.Warn(
			"GC-Sync-Protection-Register-Max-Count-Reached",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
			zap.Int("current-count", len(m.protections)),
			zap.Int("max-count", m.maxCount),
		)
		return nil, moerr.NewSyncProtectionMaxCountNoCtx(m.maxCount)
	}

	// Check if BF data is empty
	if bfData == "" {
		logutil.Error(
			"GC-Sync-Protection-Register-Empty-BF",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
		)
		return nil, moerr.NewSyncProtectionInvalidNoCtx()
	}

	// Decode base64 BloomFilter data
	bfBytes, err := base64.StdEncoding.DecodeString(bfData)
	if err != nil {
		logutil.Error(
			"GC-Sync-Protection-Register-Decode-Error",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
			zap.Error(err),
		)
		return nil, moerr.NewSyncProtectionInvalidNoCtx()
	}

	// Unmarshal BloomFilter (using index.BloomFilter which is based on xorfilter - deterministic)
	// Validate minimum buffer length before unmarshal to avoid panic
	// Minimum size: 8 (Seed) + 4*4 (SegmentLength, SegmentLengthMask, SegmentCount, SegmentCountLength) = 24 bytes
	if len(bfBytes) < 24 {
		logutil.Error(
			"GC-Sync-Protection-Register-Invalid-BF-Size",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
			zap.Int("size", len(bfBytes)),
		)
		return nil, moerr.NewSyncProtectionInvalidNoCtx()
	}

	var bf index.BloomFilter
	if err = bf.Unmarshal(bfBytes); err != nil {
		logutil.Error(
			"GC-Sync-Protection-Register-Unmarshal-Error",
			zap.String("task-id", taskID),
			zap.String("job-id", jobID),
			zap.Error(err),
		)
		return nil, moerr.NewSyncProtectionInvalidNoCtx()
	}

	protection := &SyncProtection{
		JobID:            jobID,
		BF:               bf,
		ValidTS:          validTS,
		ExpiresAtValidTS: expiresAtValidTS,
		SoftDelete:       false,
		CreateTime:       time.Now(),
	}
	m.protections[jobID] = protection

	logutil.Info(
		"GC-Sync-Protection-Registered",
		zap.String("task-id", taskID),
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", validTS),
		zap.Int("bf-size", len(bfBytes)),
		zap.Int("total-protections", len(m.protections)),
	)
	return protection, nil
}

// RenewSyncProtection renews the valid timestamp of a sync protection
func (m *SyncProtectionManager) RenewSyncProtection(jobID string, validTS int64) error {
	m.Lock()
	defer m.Unlock()

	p, ok := m.protections[jobID]
	if !ok {
		logutil.Warn(
			"GC-Sync-Protection-Renew-Not-Found",
			zap.String("job-id", jobID),
		)
		return moerr.NewSyncProtectionNotFoundNoCtx(jobID)
	}

	if p.SoftDelete {
		logutil.Warn(
			"GC-Sync-Protection-Renew-Already-Soft-Deleted",
			zap.String("job-id", jobID),
		)
		return moerr.NewSyncProtectionSoftDeleteNoCtx(jobID)
	}

	oldValidTS := p.ValidTS
	p.ValidTS = validTS

	logutil.Debug(
		"GC-Sync-Protection-Renewed",
		zap.String("job-id", jobID),
		zap.Int64("old-valid-ts", oldValidTS),
		zap.Int64("new-valid-ts", validTS),
	)
	return nil
}

// UnregisterSyncProtection soft deletes a sync protection
// Returns error if job not found (sync job needs to handle rollback)
func (m *SyncProtectionManager) UnregisterSyncProtection(jobID string) error {
	m.Lock()
	defer m.Unlock()

	p, ok := m.protections[jobID]
	if !ok {
		logutil.Warn(
			"GC-Sync-Protection-Unregister-Not-Found",
			zap.String("job-id", jobID),
		)
		return moerr.NewSyncProtectionNotFoundNoCtx(jobID)
	}

	p.SoftDelete = true

	logutil.Info(
		"GC-Sync-Protection-Soft-Deleted",
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", p.ValidTS),
	)
	return nil
}

// ReleaseSyncProtection is the idempotent terminal form used by durable read
// leases. Durable revocation has already made the read reference unresolvable,
// so the object pin and its capacity must be removed immediately rather than
// inheriting sync-job soft-delete/checkpoint semantics.
func (m *SyncProtectionManager) ReleaseSyncProtection(jobID string) error {
	m.Lock()
	defer m.Unlock()
	p := m.protections[jobID]
	if p == nil {
		return nil
	}
	delete(m.protections, jobID)
	logutil.Info(
		"GC-Sync-Protection-Released",
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", p.ValidTS),
	)
	return nil
}

// CleanupSoftDeleted cleans up soft-deleted protections when checkpoint watermark > validTS
// This should be called during GC when processing checkpoints
func (m *SyncProtectionManager) CleanupSoftDeleted(checkpointWatermark int64) {
	m.Lock()
	defer m.Unlock()

	for jobID, p := range m.protections {
		// Condition: soft delete state AND checkpoint watermark > validTS
		if p.SoftDelete && checkpointWatermark > p.ValidTS {
			delete(m.protections, jobID)
			logutil.Info(
				"GC-Sync-Protection-Cleaned-Soft-Deleted",
				zap.String("job-id", jobID),
				zap.Int64("valid-ts", p.ValidTS),
				zap.Int64("checkpoint-watermark", checkpointWatermark),
			)
		}
	}
}

// CleanupExpired removes absolute-expiry protections at their terminal
// timestamp and renewable protections after their renewal TTL. This also
// handles crashed sync jobs that did not unregister.
func (m *SyncProtectionManager) CleanupExpired() {
	m.cleanupExpiredAt(time.Now())
}

func (m *SyncProtectionManager) cleanupExpiredAt(now time.Time) {
	m.Lock()
	defer m.Unlock()
	m.cleanupExpiredAtLocked(now)
}

func (m *SyncProtectionManager) cleanupExpiredAtLocked(now time.Time) {
	for jobID, p := range m.protections {
		validTime := time.Unix(0, p.ValidTS)

		// Fixed-lifetime capabilities expire at ValidTS. Renewable sync jobs
		// retain the existing last-renewal-plus-TTL contract.
		expired := p.ExpiresAtValidTS && !now.Before(validTime)
		if !p.ExpiresAtValidTS {
			expired = now.Sub(validTime) > m.ttl
		}
		if !p.SoftDelete && expired {
			delete(m.protections, jobID)
			logutil.Warn(
				"GC-Sync-Protection-Force-Cleaned-Expired",
				zap.String("job-id", jobID),
				zap.Int64("valid-ts", p.ValidTS),
				zap.Duration("age", now.Sub(validTime)),
				zap.Duration("ttl", m.ttl),
			)
		}
	}
}

// GetProtectionCount returns the number of protections
func (m *SyncProtectionManager) GetProtectionCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.protections)
}

// GetProtectionCountByState returns the count of protections by state
func (m *SyncProtectionManager) GetProtectionCountByState() (active, softDeleted int) {
	m.RLock()
	defer m.RUnlock()

	for _, p := range m.protections {
		if p.SoftDelete {
			softDeleted++
		} else {
			active++
		}
	}
	return
}

// HasProtection checks if a job has protection
func (m *SyncProtectionManager) HasProtection(jobID string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.protections[jobID]
	return ok
}

// IsProtected checks if an object name is protected by any BloomFilter
func (m *SyncProtectionManager) IsProtected(objectName string) bool {
	m.RLock()
	defer m.RUnlock()

	if len(m.protections) == 0 {
		return false
	}

	for _, p := range m.protections {
		// Use MayContainsKey for single element test
		if result, err := p.BF.MayContainsKey([]byte(objectName)); err == nil && result {
			return true
		}
	}
	return false
}

// FilterProtectedFiles filters out protected files from the list
// Returns files that are NOT protected (can be deleted)
func (m *SyncProtectionManager) FilterProtectedFiles(files []string) []string {
	m.RLock()
	defer m.RUnlock()

	if len(m.protections) == 0 || len(files) == 0 {
		return files
	}

	// Build result: files that are NOT protected
	result := make([]string, 0, len(files))
	protectedCount := 0

	for _, f := range files {
		protected := false

		// Check against each BloomFilter
		for _, p := range m.protections {
			if contains, err := p.BF.MayContainsKey([]byte(f)); err == nil && contains {
				protected = true
				break
			}
		}

		if protected {
			protectedCount++
		} else {
			result = append(result, f)
		}
	}

	if protectedCount > 0 {
		logutil.Info(
			"GC-Sync-Protection-Filtered",
			zap.Int("total", len(files)),
			zap.Int("can-delete", len(result)),
			zap.Int("protected", protectedCount),
		)
	}

	return result
}

// ValidateSyncProtection validates that a sync protection is valid at the given prepareTS.
// Returns nil if valid, or an error indicating the validation failure reason.
// This is called by TN during PrepareCommit to ensure the sync protection is still active.
func (m *SyncProtectionManager) ValidateSyncProtection(jobID string, prepareTS int64) error {
	m.RLock()
	defer m.RUnlock()

	protection, exists := m.protections[jobID]
	if !exists {
		return moerr.NewSyncProtectionNotFoundNoCtx(jobID)
	}

	if protection.SoftDelete {
		return moerr.NewSyncProtectionSoftDeleteNoCtx(jobID)
	}

	if protection.ValidTS < prepareTS {
		return moerr.NewSyncProtectionExpiredNoCtx(jobID, protection.ValidTS, prepareTS)
	}

	return nil
}
