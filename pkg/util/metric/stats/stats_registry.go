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
)

type Family struct {
	logExporter *LogExporter
}

// Registry holds mapping between FamilyName and Family
type Registry map[string]*Family

var DefaultRegistry = Registry{}

func Register(familyName string, opts ...Options) {
	DefaultRegistry.Register(familyName, opts...)
}

// Register registers stats family to registry
// statsFName is a unique family name for the stats
// stats is the pointer to the LogExporter object
func (r *Registry) Register(familyName string, opts ...Options) {
	if _, exists := (*r)[familyName]; exists {
		panic("Duplicate Family Name")
	}

	initOpts := defaultOptions()
	for _, optFunc := range opts {
		optFunc(&initOpts)
	}

	(*r)[familyName] = &Family{
		logExporter: initOpts.logExporter,
	}
}

func WithLogExporter(logExporter *LogExporter) Options {
	return func(o *options) {
		o.logExporter = logExporter
	}
}

type Options func(*options)

type options struct {
	logExporter *LogExporter
}

func defaultOptions() options {
	return options{}
}

// ExportLog returns the snapshot of all the Family in the registry
func (r *Registry) ExportLog() (families map[string][]zap.Field) {
	for familyName, family := range *r {
		families[familyName] = (*family.logExporter).Export()
	}
	return
}
