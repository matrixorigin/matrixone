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

package mysql

import "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

// sequenceOptions collects CREATE SEQUENCE options, which may appear in any
// order (MariaDB-compatible). Each option may be given at most once.
type sequenceOptions struct {
	incrementBy *tree.IncrementByOption
	minValue    *tree.MinValueOption
	maxValue    *tree.MaxValueOption
	startWith   *tree.StartWithOption
	cycle       bool
	cycleSet    bool
}

// merge adds the single option held by o into s. It returns the name of the
// option when it was already set, or "" on success.
func (s *sequenceOptions) merge(o *sequenceOptions) string {
	switch {
	case o.incrementBy != nil:
		if s.incrementBy != nil {
			return "INCREMENT"
		}
		s.incrementBy = o.incrementBy
	case o.minValue != nil:
		if s.minValue != nil {
			return "MINVALUE"
		}
		s.minValue = o.minValue
	case o.maxValue != nil:
		if s.maxValue != nil {
			return "MAXVALUE"
		}
		s.maxValue = o.maxValue
	case o.startWith != nil:
		if s.startWith != nil {
			return "START"
		}
		s.startWith = o.startWith
	case o.cycleSet:
		if s.cycleSet {
			return "CYCLE"
		}
		s.cycle, s.cycleSet = o.cycle, true
	}
	return ""
}
