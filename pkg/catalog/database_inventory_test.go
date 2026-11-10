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

package catalog

import (
	"math"
	"testing"
)

func TestDatabaseInventoryIncrementSQL(t *testing.T) {
	for _, tc := range []struct {
		account uint32
		id      uint64
		want    string
	}{
		{7, 42, "update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=7 and database_id=42 and epoch>0 and epoch<18446744073709551615"},
		{math.MaxUint32, math.MaxUint64, "update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=4294967295 and database_id=18446744073709551615 and epoch>0 and epoch<18446744073709551615"},
	} {
		if got := DatabaseInventoryIncrementSQL(tc.account, tc.id); got != tc.want {
			t.Fatalf("inventory increment SQL: got %q, want %q", got, tc.want)
		}
	}
}
