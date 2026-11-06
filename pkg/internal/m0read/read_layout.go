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

package m0read

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// Keep the diagnostic identity used by existing strict-reader regressions.
var ErrMissingColumnWithoutProof error = moerr.NewInternalErrorNoCtx("objectio: missing normal column without proof")

// ObjectReadLayout is a bounded Engine read result, never an ObjectIO permission.
// The owning relation/reader has already checked the contract and membership.
// A non-nil layout without evolution permits physical columns only; the narrow
// catalog list is supplied only by its authoritative system-relation owner.
type ObjectReadLayout struct {
	BlockScoped       bool
	BlockID           uint16
	ObjectName        string
	WrittenSize       uint32
	SchemaSize        uint32
	Evolution         bool
	LegacyNullColumns []uint16
}

func SelectedReadLayout(layouts []*ObjectReadLayout) *ObjectReadLayout {
	if len(layouts) == 0 {
		return nil
	}
	return layouts[0]
}
