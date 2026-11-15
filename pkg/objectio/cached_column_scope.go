// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// CachedColumnScope owns only reusable decoder headers. The caller must keep
// entries pinned throughout With. Inputs/marked flags are borrowed solely for
// the callback; no header, byte slice or metadata alias may escape that scope.
// Only privately validated immutable cache entries bind a scoped header.
type CachedColumnScope struct {
	views  []vector.Vector
	inputs []*vector.Vector
	marked []bool
}

func (s *CachedColumnScope) With(entries []fileservice.IOEntry, destinations []*vector.Vector, consume func([]*vector.Vector, []bool) error) error {
	if len(entries) != len(destinations) || consume == nil {
		return moerr.NewInvalidInputNoCtx("invalid scoped cache column inputs")
	}
	if len(s.views) != len(entries) {
		s.views = make([]vector.Vector, len(entries))
		s.inputs = make([]*vector.Vector, len(entries))
		s.marked = make([]bool, len(entries))
	}
	defer func() {
		for i, bound := range s.marked {
			if bound {
				s.views[i].Free(nil)
			}
		}
		clear(s.inputs)
		clear(s.marked)
	}()
	for i, entry := range entries {
		destination := destinations[i]
		if destination == nil {
			return moerr.NewInvalidInputNoCtx("nil scoped cache column destination")
		}
		if _, ok := entry.CachedData.(validatedVectorCacheDataMarker); !ok {
			s.inputs[i] = destination
			continue
		}
		if err := bindCachedVectorForScope(&s.views[i], entry.CachedData); err != nil {
			return err
		}
		s.marked[i] = true
		if s.views[i].GetType().Oid != destination.GetType().Oid {
			return moerr.NewInvalidInputNoCtx("scoped cache column type mismatch")
		}
		s.views[i].SetType(*destination.GetType())
		s.inputs[i] = &s.views[i]
	}
	return consume(s.inputs, s.marked)
}
