// Copyright 2022 Matrix Origin
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

package memorystorage

import (
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

type (
	ID          = memoryengine.ID
	Nullable    = memorytable.Nullable
	Transaction = memorytable.Transaction
	Tuple       = memorytable.Tuple
	Text        = memorytable.Text
	Uint        = memorytable.Uint
	Bool        = memorytable.Bool
	Time        = memorytable.Time
)

func boolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}
