// Copyright 2023 Matrix Origin
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

package stats

import (
	"go.uber.org/zap"
	"sync"
)

// Registry holds mapping between FamilyName and Family
type Registry struct {
	families map[string]*Family
	sync.RWMutex
}

func (r *Registry) Register(familyName string, opts ...Options) {
	r.Lock()
	defer r.Unlock()

	if _, exists := r.families[familyName]; exists {
		panic("Duplicate Family Name " + familyName)
	}

	initOpts := defaultOptions()
	for _, optFunc := range opts {
		optFunc(&initOpts)
	}

	r.families[familyName] = &Family{
		logExporter: initOpts.logExporter,
	}
}

// Unregister deletes the item with familyName from map.
func (r *Registry) Unregister(familyName string) {
	r.Lock()
	defer r.Unlock()

	delete(r.families, familyName)
}

// ExportLog returns the snapshot of all the Family in the registry
func (r *Registry) ExportLog() map[string][]zap.Field {
	r.RLock()
	defer r.RUnlock()

	families := make(map[string][]zap.Field)
	for familyName, family := range r.families {
		families[familyName] = family.logExporter.Export()
	}
	return families
}

// WithLogExporter registers Family with the LogExporter
func WithLogExporter(logExporter LogExporter) Options {
	return func(o *options) {
		o.logExporter = logExporter
	}
}

type Options func(*options)

type options struct {
	logExporter LogExporter
}

func defaultOptions() options {
	return options{}
}

// DefaultRegistry will be used to register all the MO Dev Stats.
var DefaultRegistry = Registry{}

// Register registers stats family to default stats registry
// familyName is a unique family name for the stats
// opts can contain logExporter  etc. for the stats.
// Usage: stats.Register("FamilyName", stats.WithLogExporter(customStatsLogExporter))
func Register(familyName string, opts ...Options) {
	DefaultRegistry.Register(familyName, opts...)
}

// Unregister unregisters the family from DefaultRegistry.
func Unregister(familyName string) {
	DefaultRegistry.Unregister(familyName)
}
