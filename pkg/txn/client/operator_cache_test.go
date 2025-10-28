// Copyright 2021-2025 Matrix Origin
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

package client

import (
	"testing"
)

func TestOperatorCache(t *testing.T) {
	tc := &txnOperator{}

	tc.Set("key1", "value1")
	val, ok := tc.Get("key1")
	if !ok {
		t.Fatalf("expected key1 to be present")
	}
	if val.(string) != "value1" {
		t.Fatalf("expected value1, got %v", val)
	}

	tc.Delete("key1")
	_, ok = tc.Get("key1")
	if ok {
		t.Fatalf("expected key1 to be deleted")
	}
}
