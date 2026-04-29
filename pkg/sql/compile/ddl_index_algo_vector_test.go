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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// handleVectorCagraIndex / handleVectorIvfpqIndex: cover the static-check
// guards (the only branches reachable without a fully-built Compile).

func TestHandleVectorCagraIndex_BadDefCount(t *testing.T) {
	s := newScope(Merge)
	err := s.handleVectorCagraIndex(nil, 0, nil, nil, nil, "db", &plan.TableDef{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cagra index table definition")
}

func TestHandleVectorCagraIndex_BadParts(t *testing.T) {
	s := newScope(Merge)
	defs := map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {Parts: nil}, // 0 parts → guard fires
		catalog.Cagra_TblType_Storage:  {},
	}
	err := s.handleVectorCagraIndex(nil, 0, nil, nil, defs, "db", &plan.TableDef{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "part must be 1")
}

func TestHandleVectorIvfpqIndex_BadDefCount(t *testing.T) {
	s := newScope(Merge)
	err := s.handleVectorIvfpqIndex(nil, 0, nil, nil, nil, "db", &plan.TableDef{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid ivfpq index table definition")
}

func TestHandleVectorIvfpqIndex_BadParts(t *testing.T) {
	s := newScope(Merge)
	defs := map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Metadata: {Parts: nil}, // 0 parts → guard fires
		catalog.Ivfpq_TblType_Storage:  {},
	}
	err := s.handleVectorIvfpqIndex(nil, 0, nil, nil, defs, "db", &plan.TableDef{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "part must be 1")
}
