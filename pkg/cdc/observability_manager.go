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

package cdc

import (
	"sync"
	"time"
)

// ObservabilityManager manages all observability components for CDC
type ObservabilityManager struct {
	progressMonitor *ProgressMonitor
	mu              sync.RWMutex
	started         bool
}

var (
	globalObservabilityManager     *ObservabilityManager
	globalObservabilityManagerOnce sync.Once
)

// GetObservabilityManager returns the global observability manager (singleton)
func GetObservabilityManager() *ObservabilityManager {
	globalObservabilityManagerOnce.Do(func() {
		globalObservabilityManager = &ObservabilityManager{
			progressMonitor: NewProgressMonitor(
				30*time.Second, // Check every 30 seconds
				5*time.Minute,  // Consider stuck if no progress for 5 minutes
				30*time.Second, // Log progress every 30 seconds
			),
		}
	})
	return globalObservabilityManager
}

// Start starts the observability manager
func (om *ObservabilityManager) Start() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.started {
		return
	}

	om.progressMonitor.Start()
	om.started = true
}

// Stop stops the observability manager
func (om *ObservabilityManager) Stop() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if !om.started {
		return
	}

	om.progressMonitor.Stop()
	om.started = false
}

// GetProgressMonitor returns the progress monitor
func (om *ObservabilityManager) GetProgressMonitor() *ProgressMonitor {
	return om.progressMonitor
}

// RegisterTableStream registers a table stream for monitoring
func (om *ObservabilityManager) RegisterTableStream(stream *TableChangeStream) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if !om.started {
		om.mu.RUnlock()
		om.Start()
		om.mu.RLock()
	}

	om.progressMonitor.Register(stream.progressTracker)
}

// UnregisterTableStream unregisters a table stream from monitoring
func (om *ObservabilityManager) UnregisterTableStream(stream *TableChangeStream) {
	om.progressMonitor.Unregister(stream.progressTracker)
}
