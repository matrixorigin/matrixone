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

// Package casgc reclaims unreferenced datalink content-addressed store (CAS)
// blobs. It runs a periodic, reference-aware sweep that deletes per-account
// pinned blobs no longer reachable from any live datalink column or live
// snapshot, while a grace window protects blobs from a concurrent datalink_pin.
package casgc

import "time"

const (
	defaultInterval    = time.Hour
	defaultGraceWindow = 24 * time.Hour
)

// Config controls the datalink CAS reclamation sweep.
type Config struct {
	// Interval between full sweeps. Default 1h.
	Interval time.Duration `toml:"interval"`
	// GraceWindow protects freshly pinned blobs: only blobs whose object mtime
	// is older than now-GraceWindow are eligible for deletion, so an in-flight
	// datalink_pin() racing the sweep is never collected. Default 24h.
	GraceWindow time.Duration `toml:"grace-window"`
}

// Adjust fills in defaults for any unset (non-positive) field.
func (c *Config) Adjust() {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.GraceWindow <= 0 {
		c.GraceWindow = defaultGraceWindow
	}
}
