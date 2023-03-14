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

package metric

import "go.uber.org/zap"

type LogExporter interface {
	Export() []zap.Field
}

// LogExporterRegistry holds mapping between StatsFamily and there LogExporter
type LogExporterRegistry struct {
	registry map[string]*LogExporter
}

// DefaultLogExporterRegistry will be used for registering all the Developer Stats
var DefaultLogExporterRegistry = LogExporterRegistry{}

// Register registers stats family to registry
// statsFName is the family name of the stats
// stats is the pointer to the LogExporter object
func (r *LogExporterRegistry) Register(statsFName string, logExporter *LogExporter) {
	if _, exists := r.registry[statsFName]; exists {
		panic("Duplicate Stats Family Name")
	}
	r.registry[statsFName] = logExporter
}

// Gather returns the snapshot of all the statsFamily in the registry
func (r *LogExporterRegistry) Gather() (statsFamilies map[string][]zap.Field) {
	for statsFName, statsCollector := range r.registry {
		statsFamilies[statsFName] = (*statsCollector).Export()
	}
	return
}
