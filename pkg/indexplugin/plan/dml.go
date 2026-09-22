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

package plan

import planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"

// DMLMaintenanceNoOpHook is an optional capability for indexes whose hidden
// state is a pure function of every stored input in the base-table row. The
// returned columns form a conservative proof: SQL NULL-safe equality for every
// returned column, including row identity/doc identity when it is an input,
// must imply byte-for-byte-equivalent hidden-index input between the old and
// final row images. Implementations must return supported=false when type
// comparison rules, external state, or any other dependency can change the
// generated entries despite that equality result.
type DMLMaintenanceNoOpHook interface {
	DMLMaintenanceNoOpColumns(tableDef *planpb.TableDef, indexDef *planpb.IndexDef) (columns []string, supported bool, err error)
}
