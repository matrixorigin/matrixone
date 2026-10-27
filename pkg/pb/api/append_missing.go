// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (s *SchemaExtra) ValidateAppendMissing() error {
	if s == nil {
		return nil
	}
	if err := plan.ValidateAppendMissingState(s.AppendMissingState); err != nil {
		return err
	}
	if state := s.AppendMissingState; state != nil && s.NextColSeqnum != state.BaseWidth+uint32(len(state.Columns)) {
		return moerr.NewInternalErrorNoCtx("append missing state: NextColSeqnum differs")
	}
	return nil
}
